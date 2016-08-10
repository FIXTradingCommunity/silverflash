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
