/**
 * Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.fixtrading.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.transport.Transport;

/**
 * Builder for a FIXP message flow handler
 * 
 * @author Don Mendelson
 *
 * @param <T> flow type to build
 * @param <B> builder type base class
 */
public interface FlowBuilder<T, B extends FlowBuilder<T, B>> {

  /**
   * Build a flow handler
   * 
   * @return instance of flow handler
   */
  T build();

  /**
   * Supply the outbound keepalive interval
   * 
   * @param outboundKeepaliveInterval interval in milliseconds
   * @return this Builder
   */
  B withKeepaliveInterval(int outboundKeepaliveInterval);

  /**
   * Supply an encoder for FIXP session messages
   * 
   * @param encoder
   * @return this Builder
   */
  B withMessageEncoder(MessageEncoder encoder);

  /**
   * Supply an event reactor
   * 
   * @param reactor dispatches events
   * @return this Builder
   */
  B withReactor(EventReactor<ByteBuffer> reactor);

  /**
   * Supply a message sequencer
   * 
   * @param sequencer tracks sequence of messages
   * @return this Builder
   */
  B withSequencer(Sequencer sequencer);

  /**
   * Supplies a session ID
   * 
   * @param sessionId unique identifer
   * @return this Builder
   */
  B withSessionId(UUID sessionId);

  /**
   * Adds an exception handler
   * 
   * @param exceptionHandler a handler for exceptions thrown from an inner context
   * @return this Builder
   */
  B withExceptionConsumer(ExceptionConsumer exceptionHandler);

  /**
   * Supplies a transport for the session
   * 
   * @param transport a transport for messages
   * @return this Builder
   */
  B withTransport(Transport transport);

}
