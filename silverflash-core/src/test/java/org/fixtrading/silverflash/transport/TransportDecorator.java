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
