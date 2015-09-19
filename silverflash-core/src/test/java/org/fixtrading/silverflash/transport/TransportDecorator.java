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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportConsumer;

public class TransportDecorator implements Transport {
  private final Transport component;
  private final boolean isFifo;

  /**
   * Wrap a Transport
   * 
   * @param component a Transport to wrap
   * @param isFifo override attribute of the Transport
   */
  public TransportDecorator(Transport component, boolean isFifo) {
    this.component = component;
    this.isFifo = isFifo;
  }

  public void close() {
    component.close();
  }

  public boolean isFifo() {
    return isFifo;
  }

  public void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException {
    component.open(buffers, consumer);
  }

  public int read() throws IOException {
    return component.read();
  }

  public int write(ByteBuffer src) throws IOException {
    return component.write(src);
  }

  public long write(ByteBuffer[] srcs) throws IOException {
    return component.write(srcs);
  }

  @Override
  public boolean isOpen() {
    return component.isOpen();
  }
}
