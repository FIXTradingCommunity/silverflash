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

package io.fixprotocol.silverflash.fixp.flow;

import io.fixprotocol.silverflash.Receiver;

/**
 * Receives messages on FIXP session flow
 * 
 * @author Don Mendelson
 *
 */
public interface FlowReceiver extends Receiver {

  /**
   * Returns whether a heartbeat is past due from a peer. Side effect: resets the value for the next
   * heartbeat interval.
   * 
   * @return Returns {@code true} if a heartbeat is due because neither an application message nor
   *         heartbeat message was received in the last interval.
   */
  boolean isHeartbeatDue();

}
