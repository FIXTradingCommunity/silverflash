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

package org.fixtrading.silverflash.examples;


/**
 * Configuration for sessions
 * 
 * @author Don Mendelson
 *
 */
public interface SessionConfigurationService {

  boolean isOutboundFlowRecoverable();

  boolean isOutboundFlowSequenced();

  int getKeepaliveInterval();

  byte[] getCredentials();

  /**
   * Multiplex multiple FIXP sessions on a single transport
   * @return Returns {@code true} if transport is multiplexed
   */
  boolean isTransportMultiplexed();
}
