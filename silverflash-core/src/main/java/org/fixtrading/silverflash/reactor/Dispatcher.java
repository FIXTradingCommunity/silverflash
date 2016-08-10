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

package org.fixtrading.silverflash.reactor;


import java.io.IOException;

import org.fixtrading.silverflash.Receiver;

/**
 * Dispatches events to a receiver
 * 
 * @author Don Mendelson
 *
 */
@FunctionalInterface
public interface Dispatcher<T> {

  /**
   * Dispatch an event
   * 
   * @param topic category of the event
   * @param payload a message payload
   * @param receiver target of the event
   * @throws IOException if an IO error occurs
   */
  void dispatch(Topic topic, T payload, Receiver receiver) throws IOException;
}
