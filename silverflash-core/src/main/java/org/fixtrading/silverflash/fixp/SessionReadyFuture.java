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

package org.fixtrading.silverflash.fixp;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_READY;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.fixtrading.silverflash.reactor.EventFuture;
import org.fixtrading.silverflash.reactor.EventReactor;

/**
 * A Future that notifies an observer when a session is ready to use
 * 
 * @author Don Mendelson
 *
 */
public class SessionReadyFuture extends EventFuture {

  /**
   * Constructor
   * 
   * @param sessionId session identifier
   * @param reactor an EventReactor
   */
  public SessionReadyFuture(UUID sessionId, EventReactor<ByteBuffer> reactor) {
    super(SessionEventTopics.getTopic(sessionId, SESSION_READY), reactor);
  }

}
