/**
 *    Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.fixtrading.silverflash.transport;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.fixtrading.silverflash.util.platform.AffinityThreadFactory;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * Provides a pair of shared memory pipes for bidirectional in-memory communications
 * <p>
 * This implementation is only guaranteed to be safe for a single reader and writer in each
 * direction. Pipes are selected by "client" and "server" roles--each one end of this Transport must
 * play one role and the other end must play the opposite role
 * 
 * 
 * @author Don Mendelson
 *
 */
@SuppressWarnings("restriction")
public class SharedMemoryTransport implements Transport {

  private class Channel {
    private long baseAddress;
    private MappedByteBuffer bb;
    private long maxOffset;
    private RandomAccessFile memoryMappedFile;

    public Channel(File file, int requestedFileSize) throws IOException {
      createMemoryMappedFile(file, requestedFileSize);
    }

    public void close() throws IOException {
      memoryMappedFile.close();
    }

    public boolean isOpen() {
      return memoryMappedFile.getChannel().isOpen();
    }

    protected long getWritePosition() {
      return readLong(WRITE_PTR_OFFSET);
    }

    protected long incrementLong(long position, int value) {
      return UNSAFE.getAndAddLong(null, baseAddress + position, value);
    }

    protected long readLong(long position) {
      return UNSAFE.getLongVolatile(null, baseAddress + position);
    }

    long getBufferOffset(long position) {
      return baseAddress + DATA_OFFSET + (position & offsetMask);
    }

    long getSpaceRemaining(long bufferOffset) {
      return maxOffset - bufferOffset;
    }

    private void createMemoryMappedFile(File file, int requestedFileSize) throws IOException {
      boolean existed = file.exists();
      this.memoryMappedFile = new RandomAccessFile(file, "rw");
      FileChannel channel = memoryMappedFile.getChannel();

      long fileSize;
      if (existed) {
        fileSize = memoryMappedFile.length();
      } else {
        if (requestedFileSize < 0 || Integer.bitCount(requestedFileSize - DATA_OFFSET) != 1) {
          throw new IllegalArgumentException("Invalid file size");
        }
        fileSize = requestedFileSize & 0x00000000ffffffffL;
      }
      offsetMask = getOffsetMask(fileSize - DATA_OFFSET);

      bb = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
      bb.order(ByteOrder.nativeOrder());
      baseAddress = ((DirectBuffer) bb).address();

      if (!existed) {
        // memset() to 0
        UNSAFE.setMemory(baseAddress, fileSize, (byte) 0);
      }
      if (!existed || reset) {
        initializePositions();
      }

      this.maxOffset = getMaxBufferOffset(file.length());
    }

    private long getMaxBufferOffset(long fileLength) {
      return baseAddress + DATA_OFFSET + ((fileLength - DATA_OFFSET - 1) & offsetMask) + 1;
    }

    private void initializePositions() {
      writeLong(WRITE_PTR_OFFSET, 0);
      writeLong(READ_PTR_OFFSET, 0);
    }

    private void writeLong(long position, long value) {
      UNSAFE.putLongVolatile(null, baseAddress + position, value);
    }
  }

  /**
   * Dispatcher thread passes messages to consumers
   */
  private class Dispatcher implements Runnable {

    private final AtomicBoolean isRunning = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private Thread thread;

    private final CopyOnWriteArrayList<SharedMemoryTransport> transports =
        new CopyOnWriteArrayList<>();

    public void addTransport(SharedMemoryTransport transport) {
      transports.add(transport);
      if (isRunning.compareAndSet(false, true)) {
        thread = SharedMemoryTransport.this.threadFactory.newThread(this);
        thread.start();
        while (!started.compareAndSet(true, true));
      }
    }

    public void removeTransport(SharedMemoryTransport transport) {
      transports.remove(transport);
      if (transports.isEmpty()) {
        isRunning.set(false);
        if (thread != null) {
          try {
            thread.join(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }

    public void run() {
      started.set(true);
      while (isRunning.compareAndSet(true, true)) {
        for (int i = 0; i < transports.size(); i++) {
          try {
            final SharedMemoryTransport transport = transports.get(i);
            if (transport.isOpen()) {
              final long writePosition = transport.getReadChannel().getWritePosition();
              final long readPosition = getReadChannel().getReadPosition();
              if (writePosition > readPosition) {
                try {
                  transport.read();
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            }
          } catch (ArrayIndexOutOfBoundsException ex) {
            // item removed while iterating - retry iteration
          }
        }
      }
      started.set(false);
    }
  }

  private class SinkChannel extends Channel {

    /**
     * @param file
     * @param requestedFileSize
     * @throws IOException
     */
    public SinkChannel(File file, int requestedFileSize) throws IOException {
      super(file, requestedFileSize);
    }

    /**
     * @param src
     * @return
     * @throws IOException
     */
    public int write(ByteBuffer src) throws IOException {
      int pos = src.position();
      int bytesToWrite = src.remaining();
      writeBuffer(src, getWritePosition(), bytesToWrite);
      incrementWritePosition(bytesToWrite);
      src.position(pos + bytesToWrite);
      return bytesToWrite;
    }

    public long write(ByteBuffer[] srcs) throws IOException {
      int bytesWritten = 0;
      for (int i = 0; i < srcs.length; i++) {
        if (srcs[i] == null) {
          break;
        }
        bytesWritten += write(srcs[i]);
      }
      return bytesWritten;
    }

    private long incrementWritePosition(int bytesToWrite) {
      return incrementLong(WRITE_PTR_OFFSET, bytesToWrite);

    }

    private void writeBuffer(ByteBuffer src, long writePosition, int length) {
      int bytesWritten = 0;
      long position = writePosition;
      while (bytesWritten < length) {
        final long bufferOffset = getBufferOffset(position);
        long remaining = getSpaceRemaining(bufferOffset);
        long bytesToWrite = (int) Math.min(remaining, length - bytesWritten);

        if (src.hasArray()) {
          byte[] srcArray = src.array();
          UNSAFE.copyMemory(srcArray, BYTE_ARRAY_BASE_OFFSET + bytesWritten, null, bufferOffset,
              bytesToWrite);
        } else {
          long srcAddress = ((DirectBuffer) src).address() + bytesWritten;
          UNSAFE.copyMemory(srcAddress, bufferOffset, bytesToWrite);
        }
        bytesWritten += bytesToWrite;
        position += bytesToWrite;
      }
    }

  }

  /**
   * @author Don Mendelson
   *
   */
  private class SourceChannel extends Channel {

    /**
     * @param file
     * @param requestedFileSize
     * @throws IOException
     */
    public SourceChannel(File file, int requestedFileSize) throws IOException {
      super(file, requestedFileSize);
    }

    /**
     * @param buffer
     * @return
     */
    public int read(ByteBuffer buffer) {
      final long readPosition = getReadPosition();
      int bytesToRead = Math.min(buffer.remaining(), (int) (getWritePosition() - readPosition));
      if (bytesToRead > 0) {
        readBuffer(buffer, readPosition, bytesToRead);
        incrementReadPosition(bytesToRead);
      }
      return bytesToRead;
    }

    private long getReadPosition() {
      return readLong(READ_PTR_OFFSET);
    }

    private long incrementReadPosition(int bytesToRead) {
      return incrementLong(READ_PTR_OFFSET, bytesToRead);
    }

    private void readBuffer(ByteBuffer dest, long readPosition, int length) {
      int bytesRead = 0;
      long position = readPosition;
      while (bytesRead < length) {
        final long bufferOffset = getBufferOffset(position);
        long remaining = getSpaceRemaining(bufferOffset);
        long bytesToRead = (int) Math.min(remaining, length - bytesRead);

        if (dest.hasArray()) {
          byte[] destArray = dest.array();
          // When the object reference is null, the offset supplies an
          // absolute base address.
          UNSAFE.copyMemory(null, bufferOffset, destArray, BYTE_ARRAY_BASE_OFFSET + bytesRead,
              bytesToRead);
        } else {
          long destAddress = ((DirectBuffer) dest).address() + bytesRead;
          UNSAFE.copyMemory(bufferOffset, destAddress, bytesToRead);
        }

        bytesRead += bytesToRead;
        position += bytesToRead;
      }
      dest.position(length);
    }

  }

  public static final String FILENAME_PATTERN = "shmemtransport%d%s.dat";
  private static int BYTE_ARRAY_BASE_OFFSET;
  private static final int DATA_OFFSET = 16;
  private static final Path DEFAULT_BASE_PATH = Paths.get(System.getProperty("user.home"),
      "session", "transport");
  private static final int DEFAULT_FILESIZE = 0x40000000 + DATA_OFFSET;
  private static final AtomicInteger fileNumber = new AtomicInteger();
  private static final int READ_PTR_OFFSET = 8;
  private static Unsafe UNSAFE;
  private static final int WRITE_PTR_OFFSET = 0;
  static {
    ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();

    try {
      Class<?> clazz = systemClassLoader.loadClass("sun.misc.Unsafe");
      Field f = clazz.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      UNSAFE = (Unsafe) f.get(null);
      BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    } catch (ClassNotFoundException | SecurityException | IllegalAccessException
        | IllegalArgumentException | NoSuchFieldException e) {
      e.printStackTrace();
    }
  }

  static long getOffsetMask(long size) {
    long offset = size - 1;
    int highestBit = Long.SIZE - 1;
    long mask = 0xffffffffffffffffL;
    long bit = 0x1000000000000000L;
    for (; highestBit >= 0; highestBit--) {
      if ((offset & bit) != 0) {
        break;
      }
      bit >>>= 1;
    }
    mask >>>= (Long.SIZE - highestBit + 2);
    return mask;
  }

  private Supplier<ByteBuffer> buffers;

  private TransportConsumer consumer;
  private final Dispatcher dispatcher = new Dispatcher();
  private final boolean isClient;
  private final AtomicBoolean isOpen = new AtomicBoolean();
  private long offsetMask;
  private SourceChannel readChannel;

  private final boolean reset;
  private final ThreadFactory threadFactory;
  private int transportNumber = 1;
  private SinkChannel writeChannel;

  public SharedMemoryTransport(boolean isClient) {
    this(isClient, false, new AffinityThreadFactory(1, true, true, "SHMEMTRAN"));
  }

  public SharedMemoryTransport(boolean isClient, boolean reset, ThreadFactory threadFactory) {
    this.isClient = isClient;
    this.reset = reset;
    this.threadFactory = threadFactory;
    transportNumber = fileNumber.incrementAndGet();
  }

  public SharedMemoryTransport(boolean isClient, int transportNumber, boolean reset) {
    this(isClient, transportNumber, reset, new AffinityThreadFactory(1, true, true, "SHMEMTRAN"));
  }

  public SharedMemoryTransport(boolean isClient, int transportNumber, boolean reset,
      ThreadFactory threadFactory) {
    this.isClient = isClient;
    this.reset = reset;
    this.threadFactory = threadFactory;
    this.transportNumber = transportNumber;
  }

  public void close() {
    if (isOpen.compareAndSet(true, false)) {
      try {
        dispatcher.removeTransport(this);

        if (readChannel != null) {
          readChannel.close();
        }
        if (writeChannel != null) {
          writeChannel.close();
        }
      } catch (IOException e) {

      }
      if (consumer != null) {
        consumer.disconnected();
      }
    }
  }

  public void connected() {
    consumer.connected();
  }

  public void disconnected() {
    consumer.disconnected();
  }

  /**
   * @return the transportNumber
   */
  public int getTransportNumber() {
    return transportNumber;
  }

  public boolean isFifo() {
    return true;
  }

  /**
   * @return returns {@code true} if this Transport is open
   */
  public boolean isOpen() {
    return isOpen.get();
  }

  public void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException {
    if (isOpen.compareAndSet(false, true)) {
      Objects.requireNonNull(buffers);
      Objects.requireNonNull(consumer);
      this.buffers = buffers;
      this.consumer = consumer;
      doOpen(isClient);

      dispatcher.addTransport(this);

      connected();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.Transport#read()
   */
  public int read() throws IOException {
    ByteBuffer buffer = buffers.get();
    buffer.clear();
    int bytesRead = getReadChannel().read(buffer);
    if (bytesRead > 0) {
      buffer.flip();
      consumer.accept(buffer);
    }
    return bytesRead;
  }

  /**
   * @param transportNumber the transportNumber to set
   */
  public void setTransportNumber(int transportNumber) {
    this.transportNumber = transportNumber;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.Transport#write(java.nio.ByteBuffer)
   */
  public int write(ByteBuffer src) throws IOException {
    src.flip();
    return getWriteChannel().write(src);
  }

  SourceChannel getReadChannel() {
    return readChannel;
  }

  SinkChannel getWriteChannel() {
    return writeChannel;
  }

  private void doOpen(boolean isClient) throws IOException {

    final Path basePath = Files.createDirectories(DEFAULT_BASE_PATH, new FileAttribute<?>[0]);
    final File baseFile = basePath.toFile();

    final File clientFile =
        new File(baseFile, String.format(FILENAME_PATTERN, transportNumber, "C"));
    final File serverFile =
        new File(baseFile, String.format(FILENAME_PATTERN, transportNumber, "S"));

    if (isClient) {
      this.readChannel = new SourceChannel(clientFile, DEFAULT_FILESIZE);
      this.writeChannel = new SinkChannel(serverFile, DEFAULT_FILESIZE);
    } else {
      this.readChannel = new SourceChannel(serverFile, DEFAULT_FILESIZE);
      this.writeChannel = new SinkChannel(clientFile, DEFAULT_FILESIZE);
    }
  }

}
