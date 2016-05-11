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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.NEW_SESSION_CREATED;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.WeakHashMap;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiationResponseDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.TopicDecoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * A collection of Session
 * 
 * @author Don Mendelson
 */
public class Sessions {

  private final Receiver newSessionHandler = new Receiver() {
    private final MessageDecoder messageDecoder = new MessageDecoder();
    final byte[] uuid = new byte[16];

    public void accept(ByteBuffer buffer) {
      Optional<Decoder> optDecoder = messageDecoder.wrap(buffer, buffer.position());
      if (optDecoder.isPresent()) {
        final Decoder decoder = optDecoder.get();
        switch (decoder.getMessageType()) {
        case NEGOTIATION_RESPONSE:
          NegotiationResponseDecoder negotiateDecoder = (NegotiationResponseDecoder) decoder;
          negotiateDecoder.getSessionId(uuid, 0);
          identifyNewSession(SessionId.UUIDFromBytes(uuid));
          break;
        case TOPIC:
          TopicDecoder topicDecoder = (TopicDecoder) decoder;
          topicDecoder.getSessionId(uuid, 0);
          identifyNewSession(SessionId.UUIDFromBytes(uuid));
          break;
        default:
          break;
        }

      }
    }

  };

  private final List<Session<UUID>> newSessions = Collections.synchronizedList(new ArrayList<>());
  private Subscription newSessionSubscription;
  private EventReactor<ByteBuffer> reactor;
  private final Map<Session<UUID>, UUID> sessionMap = Collections
      .synchronizedMap(new WeakHashMap<>());

  /**
   * Add a new Session for which the ID has not been assigned yet
   * 
   * @param session
   *          to add
   */
  public void addNewSession(Session<UUID> session) {
    newSessions.add(session);
  }

  /**
   * Add a new Session
   * 
   * @param session
   *          to add
   */
  public void addSession(Session<UUID> session) {
    sessionMap.put(session, session.getSessionId());
  }

  /**
   * Returns a Session by its unique identifier
   * 
   * @param sessionId
   *          Session ID
   * @return a Session or {@code null} if it is not found
   */
  public Session<UUID> getSession(UUID sessionId) {
    // Use keyset instead of comparing values because server session ID is
    // not
    // assigned on creation.
    for (Session<UUID> session : sessionMap.keySet()) {
      if (session.getSessionId().equals(sessionId)) {
        return session;
      }
    }
    return null;
  }

  public Sessions withEventReactor(EventReactor<ByteBuffer> reactor) {
    Objects.requireNonNull(reactor);
    this.reactor = reactor;
    subscribeForNewSessions();
    return this;
  }

  void subscribeForNewSessions() {
    Topic topic = SessionEventTopics.getTopic(NEW_SESSION_CREATED);
    newSessionSubscription = reactor.subscribe(topic, newSessionHandler);
  }

  void identifyNewSession(UUID sessionId) {
    Iterator<Session<UUID>> iter = newSessions.iterator();
    while (iter.hasNext()) {
      Session<UUID> session = iter.next();
      if (session.getSessionId().equals(sessionId)) {
        addSession(session);
        iter.remove();
        break;
      }
    }
  }
}
