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

package io.fixprotocol.silverflash.fixp;

import java.io.IOException;

import io.fixprotocol.silverflash.fixp.messages.FlowType;

/**
 * Initiates a Session when a Transport connection is established
 * 
 * @author Don Mendelson
 *
 */
public interface Establisher {

  /**
   * A signal to this Establisher to complete its work
   * @throws IOException if an IO error occurred while completing protocol
   * establishment
   */
  void complete() throws IOException;
  
  /**
   * Transport layer established a connection
   */
  void connected();

  /**
   * @return type of the inbound flow of a Session
   */
  FlowType getInboundFlow();

  /**
   * @return expected inbound heartbeat interval (milliseconds)
   */
  long getInboundKeepaliveInterval();

  /**
   * @return type of the outbound flow of a Session
   */
  FlowType getOutboundFlow();

  /**
   * @return outbound heartbeat interval (milliseconds)
   */
  long getOutboundKeepaliveInterval();

  /**
   * @return Session ID serialized to a byte array
   */
  byte[] getSessionId();

  /**
   * Sets heartbeat interval
   * 
   * @param outboundKeepaliveInterval heartbeat interval in milliseconds
   * @return this Establisher
   */
  Establisher withOutboundKeepaliveInterval(int outboundKeepaliveInterval);

}
