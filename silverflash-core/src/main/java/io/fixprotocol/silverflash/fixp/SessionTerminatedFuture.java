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

import static io.fixprotocol.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;

import java.nio.ByteBuffer;
import java.util.UUID;

import io.fixprotocol.silverflash.reactor.EventFuture;
import io.fixprotocol.silverflash.reactor.EventReactor;

/**
 * A Future that notifies an observer when a session has terminated
 * 
 * @author Don Mendelson
 *
 */
public class SessionTerminatedFuture extends EventFuture {

  /**
   * Constructor
   * 
   * @param sessionId session identifier
   * @param reactor an EventReactor
   */
  public SessionTerminatedFuture(UUID sessionId, EventReactor<ByteBuffer> reactor) {
    super(SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED), reactor);
  }

}
