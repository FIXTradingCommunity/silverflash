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

package org.fixtrading.silverflash;

import java.nio.ByteBuffer;



/**
 * Consumes messages received on a Session (application layer)
 * 
 * @author Don Mendelson
 * 
 * @param T session identifier type
 */
@FunctionalInterface
public interface MessageConsumer<T> {

  /**
   * Consume a message
   * 
   * @param message buffer holding a message
   * @param session session that delivered a message
   * @param seqNo sequence number or zero if the flow is unsequenced
   */
  void accept(ByteBuffer message, Session<T> session, long seqNo);
}
