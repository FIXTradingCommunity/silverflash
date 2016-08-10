/**
 *    Copyright 2015-2016 FIX Protocol Ltd
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.fixtrading.silverflash.buffer.BufferSupplier;

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

    long getBufferOffset(long position) {
      return baseAddress + DATA_OFFSET + (position & offsetMask);
    }

    private long getMaxBufferOffset(long fileLength) {
      return baseAddress + DATA_OFFSET + ((fileLength - DATA_OFFSET - 1) & offsetMask) + 1;
    }

    long getSpaceRemaining(long bufferOffset) {
      return maxOffset - bufferOffset;
    }

    protected long getWritePosition() {
      return readLong(WRITE_PTR_OFFSET);
    }

    protected long incrementLong(long position, int value) {
      return UNSAFE.getAndAddLong(null, baseAddress + position, value);
    }

    private void initializePositions() {
      writeLong(WRITE_PTR_OFFSET, 0);
      writeLong(READ_PTR_OFFSET, 0);
    }

    public boolean isOpen() {
      return memoryMappedFile.getChannel().isOpen();
    }

    protected long readLong(long position) {
      return UNSAFE.getLongVolatile(null, baseAddress + position);
    }

    private void writeLong(long position, long value) {
      UNSAFE.putLongVolatile(null, baseAddress + position, value);
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

    private long incrementWritePosition(int bytesToWrite) {
      return incrementLong(WRITE_PTR_OFFSET, bytesToWrite);

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

    private long getReadPosition() {
      return readLong(READ_PTR_OFFSET);
    }

    private long incrementReadPosition(int bytesToRead) {
      return incrementLong(READ_PTR_OFFSET, bytesToRead);
    }

    public boolean isReadyToRead() {
      final long writePosition = getWritePosition();
      final long readPosition = getReadPosition();
      return writePosition > readPosition;
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

  private static int BYTE_ARRAY_BASE_OFFSET;
  private static final int DATA_OFFSET = 16;
  private static final Path DEFAULT_BASE_PATH = Paths.get(System.getProperty("user.home"),
      "session", "transport");
  private static final int DEFAULT_FILESIZE = 0x40000000 + DATA_OFFSET;
  public static final String FILENAME_PATTERN = "shmemtransport%d%s.dat";
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
  private final Dispatcher dispatcher;
  private final boolean isClient;
  private final AtomicBoolean isOpen = new AtomicBoolean();
  private long offsetMask;
  private SourceChannel readChannel;

  private final boolean reset;
  private int transportNumber = 1;
  private SinkChannel writeChannel;

  public SharedMemoryTransport(boolean isClient) {
    this(isClient, false);
  }

  public SharedMemoryTransport(boolean isClient, boolean reset) {
    this(isClient, reset, fileNumber.incrementAndGet());
  }

  public SharedMemoryTransport(boolean isClient, boolean reset, int transportNumber) {
    this(isClient, reset, transportNumber, new Dispatcher());
  }

  public SharedMemoryTransport(boolean isClient, boolean reset, int transportNumber,
      Dispatcher dispatcher) {
    this.isClient = isClient;
    this.reset = reset;
    this.transportNumber = transportNumber;
    this.dispatcher = dispatcher;
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

  private void doOpen(boolean isClient) throws IOException {

    final Path basePath = Files.createDirectories(DEFAULT_BASE_PATH, new FileAttribute<?>[0]);
    final File baseFile = basePath.toFile();

    final File clientFile = new File(baseFile,
        String.format(FILENAME_PATTERN, transportNumber, "C"));
    final File serverFile = new File(baseFile,
        String.format(FILENAME_PATTERN, transportNumber, "S"));

    if (isClient) {
      this.readChannel = new SourceChannel(clientFile, DEFAULT_FILESIZE);
      this.writeChannel = new SinkChannel(serverFile, DEFAULT_FILESIZE);
    } else {
      this.readChannel = new SourceChannel(serverFile, DEFAULT_FILESIZE);
      this.writeChannel = new SinkChannel(clientFile, DEFAULT_FILESIZE);
    }
  }

  SourceChannel getReadChannel() {
    return readChannel;
  }

  /**
   * @return the transportNumber
   */
  public int getTransportNumber() {
    return transportNumber;
  }

  SinkChannel getWriteChannel() {
    return writeChannel;
  }

  public boolean isFifo() {
    return true;
  }

  public boolean isMessageOriented() {
    return true;
  }

  /**
   * @return returns {@code true} if this Transport is open
   */
  public boolean isOpen() {
    return isOpen.get();
  }

  public boolean isReadyToRead() {
    return isOpen() && getReadChannel().isReadyToRead();
  }

  public CompletableFuture<SharedMemoryTransport> open(BufferSupplier buffers,
      TransportConsumer consumer) {
    CompletableFuture<SharedMemoryTransport> future = new CompletableFuture<>();

    if (isOpen.compareAndSet(false, true)) {
      Objects.requireNonNull(buffers);
      Objects.requireNonNull(consumer);
      this.buffers = buffers;
      this.consumer = consumer;

      try {
        doOpen(isClient);
        dispatcher.addTransport(this);
        connected();
        future.complete(this);
      } catch (IOException ex) {
        future.completeExceptionally(ex);
      }

    } else {
      future.complete(this);
    }
    return future;
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
   * @param transportNumber
   *          the transportNumber to set
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

}
