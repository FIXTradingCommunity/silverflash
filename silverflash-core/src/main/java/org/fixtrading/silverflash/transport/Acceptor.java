/**
 *    Copyright 2015 FIX Protocol Ltd
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

package org.fixtrading.silverflash.transport;

import java.io.IOException;
import java.util.function.Function;

import org.fixtrading.silverflash.Service;

/**
 * Listens for connections
 * 
 * @author Don Mendelson
 */
public interface Acceptor extends Service {

  /**
   * Returns a function that wraps an accepted Transport into a session of generic type
   * 
   * @param <T> wrapper class for a Transport
   * @return a function handle accepted connections
   */
  <T> Function<Transport, T> getTransportWrapper();

  /**
   * Invoked when a new connection is ready to accept
   * 
   * @return an object holding the accepted connection
   * @throws IOException if fails to establish the new connection
   */
  Transport readyToAccept() throws IOException;
}
