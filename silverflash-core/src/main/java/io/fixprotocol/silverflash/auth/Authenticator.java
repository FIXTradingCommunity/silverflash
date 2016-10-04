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

package io.fixprotocol.silverflash.auth;

import io.fixprotocol.silverflash.Service;

/**
 * Authentication service
 * 
 * @author Don Mendelson
 *
 * @param T type of session ID
 */
public interface Authenticator<T> extends Service {

  /**
   * Authenticate a session
   * 
   * @param sessionId session identifier
   * @param credentials password or key
   * @return Returns {@code true} if the session was successfully authenticated
   */
  boolean authenticate(T sessionId, byte[] credentials);

}
