package org.fixtrading.silverflash.fixp.flow;

import java.io.IOException;

import org.fixtrading.silverflash.Sender;

/**
 * A Sender for a protocol flow type
 * 
 * @author Don Mendelson
 *
 */
public interface FlowSender extends Sender {

  void sendHeartbeat() throws IOException;

  void sendEndOfStream() throws IOException;

}
