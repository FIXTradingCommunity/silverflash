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
import java.util.concurrent.CompletableFuture;

import io.fixprotocol.silverflash.buffer.BufferSupplier;
import io.fixprotocol.silverflash.transport.Transport;
import io.fixprotocol.silverflash.transport.TransportConsumer;

public class TransportDecorator implements Transport {
  private final Transport component;
  private final boolean isFifo;
  private final boolean isReadable;
  private final boolean isWritable;

  /**
   * Wrap a bidirectional Transport
   * 
   * @param component a Transport to wrap
   * @param isFifo override attribute of the Transport
   */
  public TransportDecorator(Transport component, boolean isFifo) {
    this(component, isFifo, true, true);
  }
  
  /**
   * Wrap as a unidirectional Transport
   * 
   * @param component a Transport to wrap
   * @param isFifo override attribute of the Transport
   * @param isReadable is transport readable
   * @param isWritable is transport writable
   */
  public TransportDecorator(Transport component, boolean isFifo, 
      boolean isReadable, boolean isWritable) {
    this.component = component;
    this.isFifo = isFifo;
    this.isReadable = isReadable;
    this.isWritable = isWritable;
  }

  public void close() {
    component.close();
  }

  public boolean isFifo() {
    return isFifo;
  }
  
  public boolean isMessageOriented() {
    return false;
  }

  @Override
  public boolean isOpen() {
    return component.isOpen();
  }

  /* (non-Javadoc)
   * @see io.fixprotocol.silverflash.transport.Transport#isReadyToRead()
   */
  @Override
  public boolean isReadyToRead() {
    return component.isReadyToRead();
  }

  public CompletableFuture<? extends Transport> open(BufferSupplier buffers,
      TransportConsumer consumer) {
    return component.open(buffers, consumer);
  }

  public int read() throws IOException {    
    if (isReadable) {
      return component.read();
    } else {
      throw new IOException("Transport not readable");
    }
  }

  public int write(ByteBuffer src) throws IOException {
    if (isWritable) {
      return component.write(src);
    } else {
      throw new IOException("Transport not writable");
    }
  }

  public long write(ByteBuffer[] srcs) throws IOException {
    if (isWritable) {
      return component.write(srcs);
    } else {
      throw new IOException("Transport not writable");
    }
  }
}
