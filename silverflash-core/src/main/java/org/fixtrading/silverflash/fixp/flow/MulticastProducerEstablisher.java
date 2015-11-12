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

package org.fixtrading.silverflash.fixp.flow;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.MULTICAST_TOPIC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.UUID;

import org.fixtrading.silverflash.Sender;
import org.fixtrading.silverflash.fixp.Establisher;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.TopicEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.transport.Transport;

/**
 * Implements the FIXP protocol server handshake
 * 
 * @author Don Mendelson
 *
 */
public class MulticastProducerEstablisher implements Sender, Establisher, FlowReceiver, FlowSender {
  public static final int DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL = 5000;

  private final MessageEncoder messageEncoder;
  private final FlowType outboundFlow;
  private int outboundKeepaliveInterval = DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL;
  private final EventReactor<ByteBuffer> reactor;
  private final ByteBuffer sessionMessageBuffer = ByteBuffer.allocateDirect(128).order(
      ByteOrder.nativeOrder());
  private Topic terminatedTopic;
  private final Transport transport;
  private byte[] uuidAsBytes = new byte[16];
  private final String topic;

  /**
   * Constructor
   * 
   * @param reactor an EventReactor
   * @param transport used to exchange messages with the client
   * @param outboundFlow client flow type
   * @param topic FIXP topic to publish
   * @param sessionId session ID for the topic
   * @param messageEncoder FIXP message encoder
   */
  public MulticastProducerEstablisher(EventReactor<ByteBuffer> reactor, Transport transport,
      FlowType outboundFlow, String topic, UUID sessionId, MessageEncoder messageEncoder) {
    Objects.requireNonNull(transport);
    Objects.requireNonNull(topic);
    this.reactor = reactor;
    this.transport = transport;
    this.outboundFlow = outboundFlow;
    this.topic = topic;
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
    terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);
    this.messageEncoder = messageEncoder;
  }

  @Override
  public void accept(ByteBuffer buffer) {
  }

  public void complete() {
    
  }

  @Override
  public void connected() {
    try {
      topic();
    } catch (IOException e) {
      reactor.post(terminatedTopic, null);
    }
  }

  public FlowType getInboundFlow() {
    return FlowType.NONE;
  }

  public int getInboundKeepaliveInterval() {
    return 0;
  }

  public FlowType getOutboundFlow() {
    return outboundFlow;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Establisher#getOutboundKeepaliveInterval()
   */
  public int getOutboundKeepaliveInterval() {
    return outboundKeepaliveInterval;
  }

  @Override
  public byte[] getSessionId() {
    return uuidAsBytes;
  }

  public boolean isHeartbeatDue() {
    return false;
  }

  @Override
  public long send(ByteBuffer message) throws IOException {
    Objects.requireNonNull(message);
    transport.write(message);
    return 0;
  }

  public void sendEndOfStream() throws IOException {

  }

  public void sendHeartbeat() throws IOException {

  }

   void topic() throws IOException {
     TopicEncoder topicEncoder =
        (TopicEncoder) messageEncoder.wrap(sessionMessageBuffer, 0,
            MessageType.TOPIC);
     topicEncoder.setSessionId(uuidAsBytes);
     topicEncoder.setFlow(outboundFlow);
     topicEncoder.setClassification(topic.getBytes());
    send(sessionMessageBuffer);

    Topic initTopic = SessionEventTopics.getTopic(MULTICAST_TOPIC, topic);
    reactor.post(initTopic, sessionMessageBuffer);
  }

   /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.Establisher#withOutboundKeepaliveInterval(int)
   */
  public MulticastProducerEstablisher withOutboundKeepaliveInterval(int outboundKeepaliveInterval) {
    this.outboundKeepaliveInterval = outboundKeepaliveInterval;
    return this;
  }

}
