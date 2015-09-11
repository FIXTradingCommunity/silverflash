package org.fixtrading.silverflash.reactor;

import java.nio.ByteBuffer;

import org.fixtrading.silverflash.Receiver;

/**
 * Dispatches ByteBuffer to receivers
 * 
 * @author Don Mendelson
 *
 */
public class ByteBufferDispatcher implements Dispatcher<ByteBuffer> {


  public void dispatch(Topic topic, ByteBuffer payload, Receiver receiver) {
    payload.flip();
    receiver.accept(payload);
  }

}
