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

package io.fixprotocol.silverflash.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.fixprotocol.silverflash.buffer.BufferSupplier;

/**
 * Provides a pair of pipes for bidirectional in-memory communications
 * 
 * @author Don Mendelson
 *
 */
public class PipeTransport {

  private abstract class HalfPipeTransport implements ReactiveTransport {
    private Supplier<ByteBuffer> buffers;
    private TransportConsumer consumer;

    abstract Pipe.SourceChannel getReadChannel();

    abstract Pipe.SinkChannel getWriteChannel();

    public CompletableFuture<? extends Transport> open(BufferSupplier buffers,
        TransportConsumer consumer) {
      Objects.requireNonNull(buffers);
      Objects.requireNonNull(consumer);
      CompletableFuture<Transport> future = new CompletableFuture<>();

      this.buffers = buffers;
      this.consumer = consumer;
      
      try {
        PipeTransport.this.open();
        register();
        connected();
      } catch (IOException ex) {
        future.completeExceptionally(ex);
        return future;
      }

      future.complete(this);
      return future;
    }

    public boolean isOpen() {
      return getReadChannel().isOpen();
    }

    public boolean isReadyToRead() {
      return isOpen();
    }
    
    public void close() {
      try {
        getReadChannel().close();
      } catch (IOException e) {

      }
      if (consumer != null) {
        consumer.disconnected();
      }
    }

    private void register() throws IOException {
      getReadChannel().configureBlocking(false);
      getReadChannel().register(selector, SelectionKey.OP_READ, this);
    }


    public void readyToRead() {
      try {
        read();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    public void readyToWrite() {

    }

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

    public long write(ByteBuffer[] srcs) throws IOException {
      int i = 0;
      for (i = 0; i < srcs.length; i++) {
        if (srcs[i] == null) {
          break;
        }
        srcs[i].flip();
      }
      return getWriteChannel().write(srcs, 0, i);
    }

    public int write(ByteBuffer src) throws IOException {
      src.flip();
      return getWriteChannel().write(src);
    }

    public void connected() {
      consumer.connected();
    }

    public void disconnected() {
      consumer.disconnected();
    }

    public boolean isFifo() {
      return true;
    }

    public boolean isMessageOriented() {
       return false;
    }

  }

  private final Transport clientTransport = new HalfPipeTransport() {

    Pipe.SourceChannel getReadChannel() {
      return outboundSource;
    }

    Pipe.SinkChannel getWriteChannel() {
      return inboundSink;
    }
  };

  private Pipe.SinkChannel inboundSink;
  private Pipe.SourceChannel inboundSource;
  private Pipe.SinkChannel outboundSink;
  private Pipe.SourceChannel outboundSource;
  private final Selector selector;

  private final Transport serverTransport = new HalfPipeTransport() {

    Pipe.SourceChannel getReadChannel() {
      return inboundSource;
    }

    Pipe.SinkChannel getWriteChannel() {
      return outboundSink;
    }

  };

  public PipeTransport(Selector selector) {
    Objects.requireNonNull(selector);
    this.selector = selector;
  }

  public Transport getClientTransport() {
    return clientTransport;
  }

  public Transport getServerTransport() {
    return serverTransport;
  }

  private synchronized void open() throws IOException {
    if (inboundSource == null) {
      SelectorProvider provider = SelectorProvider.provider();
      Pipe inboundPipe = provider.openPipe();
      inboundSource = inboundPipe.source();
      inboundSink = inboundPipe.sink();
      Pipe outboundPipe = provider.openPipe();
      outboundSource = outboundPipe.source();
      outboundSink = outboundPipe.sink();
    }
  }


}
