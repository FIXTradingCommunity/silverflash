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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.NEW_SESSION_CREATED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.MULTICAST_TOPIC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.fixtrading.silverflash.fixp.Establisher;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.TopicDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.transport.Transport;

/**
 * Implements the FIXP protocol client handshake
 * 
 * @author Don Mendelson
 *
 */
public class MulticastConsumerEstablisher implements Establisher, FlowReceiver, FlowSender {

  private FlowType inboundFlow;
  private int inboundKeepaliveInterval;
  private final MessageDecoder messageDecoder = new MessageDecoder();
  private final EventReactor<ByteBuffer> reactor;
  private UUID sessionId;
  private byte [] topic;
  private final Transport transport;
  private final byte[] uuidAsBytes = new byte[16];

  /**
   * Constructor
   * 
   * @param reactor
   *          an EventReactor
   * @param transport
   *          used to exchange messages with the server
   */
  public MulticastConsumerEstablisher(EventReactor<ByteBuffer> reactor, Transport transport) {
    this.reactor = reactor;
    this.transport = transport;
  }

  @Override
  public void accept(ByteBuffer buffer) {
    Optional<Decoder> optDecoder = messageDecoder.wrap(buffer, buffer.position());
    if (optDecoder.isPresent()) {
      final Decoder decoder = optDecoder.get();
      if (MessageType.TOPIC == decoder.getMessageType()) {
        onTopic((TopicDecoder) decoder);
      }
    }
  }

  public void complete() {
    
  }

  @Override
  public void connected() {
  }

  @Override
  public FlowType getInboundFlow() {
    return inboundFlow;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Establisher#getInboundKeepaliveInterval()
   */
  public int getInboundKeepaliveInterval() {
    return inboundKeepaliveInterval;
  }

  @Override
  public FlowType getOutboundFlow() {
    return FlowType.NONE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Establisher#getOutboundKeepaliveInterval()
   */
  public int getOutboundKeepaliveInterval() {
    return 0;
  }

  public byte[] getSessionId() {
    return uuidAsBytes;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.flow.FlowReceiver#isHeartbeatDue()
   */
  @Override
  public boolean isHeartbeatDue() {
    return false;
  }

  void onTopic(TopicDecoder topicDecoder) {
    final ByteBuffer buffer = topicDecoder.getBuffer();
    buffer.mark();
    topicDecoder.getSessionId(this.uuidAsBytes, 0);
    this.sessionId = SessionId.UUIDFromBytes(uuidAsBytes);
    FlowType aFlow = topicDecoder.getFlow();
    byte[] aTopic = new byte[topic.length];
    topicDecoder.getClassfication(aTopic, 0);
    if (Arrays.equals(topic, aTopic)) {
      inboundFlow = aFlow;
      Topic readyTopic = SessionEventTopics.getTopic(MULTICAST_TOPIC, new String(topic));
      buffer.reset();
      ByteBuffer bufferCopy = buffer.duplicate();
      reactor.post(readyTopic, buffer);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.Establisher#withOutboundKeepaliveInterval(int)
   */
  @Override
  public Establisher withOutboundKeepaliveInterval(int outboundKeepaliveInterval) {
    return this;
  }

  /**
   * Sets the topic for session
   * 
   * @param topic
   *          category of business messages
   * @return this MulticastConsumerEstablisher
   */
  public MulticastConsumerEstablisher withTopic(String topic) {
    Objects.requireNonNull(topic);
    this.topic = topic.getBytes();
    return this;
  }
  
  void publishNewSession(ByteBuffer topicMessage) {
    Topic topic = SessionEventTopics.getTopic(NEW_SESSION_CREATED);
    reactor.post(topic, topicMessage);
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.Sender#send(java.nio.ByteBuffer)
   */
  @Override
  public long send(ByteBuffer message) throws IOException {
    return 0;
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.fixp.flow.FlowSender#sendHeartbeat()
   */
  @Override
  public void sendHeartbeat() throws IOException {
     
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.fixp.flow.FlowSender#sendEndOfStream()
   */
  @Override
  public void sendEndOfStream() throws IOException {

  }

}
