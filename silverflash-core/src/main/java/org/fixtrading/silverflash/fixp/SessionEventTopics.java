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

package org.fixtrading.silverflash.fixp;

import java.util.UUID;

import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.Topics;

/**
 * Topics for session events
 * 
 * @author Don Mendelson
 *
 */
public final class SessionEventTopics {

  /**
   * SessionEventTopics published by a FixpSession to an application
   */
  public enum FromSessionEventType {
    /**
     * A message gap was detected on an idempotent flow.
     */
    NOT_APPLIED,
    /**
     * Logical flow ended; session may not be restarted
     */
    SESSION_FINISHED,
    /**
     * A session was established and it is ready to send and receive application messages.
     */
    SESSION_READY,
    /**
     * Transport was unbound; messages may not be sent unless session re-established
     */
    SESSION_SUSPENDED,
  }

  /**
   * SessionEventTopics published to a service provider
   */
  public enum ServiceEventType {
    /**
     * Authenticate a client
     */
    SERVICE_AUTHENTICATE,
    /**
     * Retrieve credentials for authentication
     */
    SERVICE_CREDENTIALS_REQUEST,
    /**
     * Response with retrieved credentials
     */
    SERVICE_CREDENTIALS_RETRIEVED,
    /**
     * Retrieve messages from a store
     */
    SERVICE_STORE_RETREIVE,
    /**
     * A new session was negotiated between client and server
     */
    NEW_SESSION_CREATED
  }

  /**
   * SessionEventTopics published and handled by a FixpSession
   */
  public enum SessionEventType {
    /**
     * A client session was established
     */
    CLIENT_ESTABLISHED,
    /**
     * A client session was negotiated
     */
    CLIENT_NEGOTIATED,
    /**
     * FixpSession sends heartbeat
     */
    HEARTBEAT,
    /**
     * FixpSession expects heartbeat from peer
     */
    PEER_HEARTBEAT,
    /**
     * FixpSession peer terminated flow
     */
    PEER_TERMINATED,
    /**
     * A server session was established
     */
    SERVER_ESTABLISHED,
    /**
     * A server session was negotiated. This event reports the UUID.
     */
    SERVER_NEGOTIATED,
    /**
     * A FIXP Topic for multicast was received
     */
    MULTICAST_TOPIC
  }

  /**
   * SessionEventTopics published by an application to a FixpSession
   */
  public enum ToSessionEventType {
    /**
     * Send a message asynchronously
     */
    APPLICATION_MESSAGE_TO_SEND,
    /**
     * A client was authenticated
     */
    AUTHENTICATED,
    /**
     * Client authentication was denied
     */
    NOT_AUTHENTICATED,
  }

  /**
   * Returns a Topic by service type
   * 
   * @param serviceEventType type of service
   * @return a Topic
   */
  public static Topic getTopic(ServiceEventType serviceEventType) {
    return Topics.getTopic(serviceEventType.name());
  }

  /**
   * Returns a Topic by event type for a session that has not yet been assigned a {@code SessionId}
   * 
   * @param sessionEventType type of event
   * @param hashCode a unique identifier, for lack of a {@code SessionId}
   * @return a Topic
   */
  public static Topic getTopic(SessionEventType sessionEventType, int hashCode) {
    return Topics.getTopic(Integer.toString(hashCode), sessionEventType.name());
  }

  /**
   * Returns a Topic by event type for a session that has not yet been assigned a {@code SessionId}
   * 
   * @param sessionEventType type of event
   * @param topicName a unique topic name
   * @return a Topic
   */
  public static Topic getTopic(SessionEventType sessionEventType, String topicName) {
    return Topics.getTopic(topicName, sessionEventType.name());
  }
  /**
   * Returns a Topic by session and event type
   * 
   * @param sessionId session identifier
   * @param sessionEventType type of event
   * @return a Topic
   */
  public static Topic getTopic(UUID sessionId, FromSessionEventType sessionEventType) {
    return Topics.getTopic(sessionId.toString(), sessionEventType.name());
  }

  /**
   * Returns a Topic by session and event type
   * 
   * @param sessionId session identifier
   * @param sessionEventType type of event
   * @return a Topic
   */
  public static Topic getTopic(UUID sessionId, SessionEventType sessionEventType) {
    return Topics.getTopic(sessionId.toString(), sessionEventType.name());
  }

  /**
   * Returns a Topic by session and event type
   * 
   * @param sessionId session identifier
   * @param sessionEventType type of event
   * @return a Topic
   */
  public static Topic getTopic(UUID sessionId, ToSessionEventType sessionEventType) {
    return Topics.getTopic(sessionId.toString(), sessionEventType.name());
  }
}
