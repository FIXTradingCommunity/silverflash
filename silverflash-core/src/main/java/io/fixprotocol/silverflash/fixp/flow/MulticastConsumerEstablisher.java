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

package io.fixprotocol.silverflash.fixp.flow;

import static io.fixprotocol.silverflash.fixp.SessionEventTopics.ServiceEventType.NEW_SESSION_CREATED;
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.SessionEventType.MULTICAST_TOPIC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.fixp.Establisher;
import io.fixprotocol.silverflash.fixp.SessionEventTopics;
import io.fixprotocol.silverflash.fixp.SessionId;
import io.fixprotocol.silverflash.fixp.messages.FlowType;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderDecoder;
import io.fixprotocol.silverflash.fixp.messages.TopicDecoder;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.reactor.Topic;
import io.fixprotocol.silverflash.transport.Transport;

/**
 * Implements the FIXP protocol client handshake
 * 
 * @author Don Mendelson
 *
 */
public class MulticastConsumerEstablisher implements Establisher, FlowReceiver, FlowSender {

  private FlowType inboundFlow;
  private int inboundKeepaliveInterval;
  private final EventReactor<ByteBuffer> reactor;
  private String topic;
  private final byte[] uuidAsBytes = new byte[16];
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final TopicDecoder topicDecoder = new TopicDecoder();
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
  }

  @Override
  public void accept(ByteBuffer buffer) {
    immutableBuffer.wrap(buffer);
    int offset = buffer.position();
    messageHeaderDecoder.wrap(immutableBuffer, offset);
    offset += messageHeaderDecoder.encodedLength();
    if (messageHeaderDecoder.schemaId() == topicDecoder.sbeSchemaId()
        && messageHeaderDecoder.templateId() == topicDecoder.sbeTemplateId()) {
      topicDecoder.wrap(immutableBuffer, offset,
          topicDecoder.sbeBlockLength(), topicDecoder.sbeSchemaVersion());
        onTopic(topicDecoder, buffer);
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
   * @see io.fixprotocol.silverflash.Establisher#getInboundKeepaliveInterval()
   */
  public long getInboundKeepaliveInterval() {
    return inboundKeepaliveInterval;
  }

  @Override
  public FlowType getOutboundFlow() {
    return FlowType.None;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.Establisher#getOutboundKeepaliveInterval()
   */
  public long getOutboundKeepaliveInterval() {
    return 0;
  }

  public byte[] getSessionId() {
    return uuidAsBytes;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.fixp.flow.FlowReceiver#isHeartbeatDue()
   */
  @Override
  public boolean isHeartbeatDue() {
    return false;
  }

  void onTopic(TopicDecoder topicDecoder, ByteBuffer buffer) {
    for (int i = 0; i < 16; i++) {
      uuidAsBytes[i] = (byte) topicDecoder.sessionId(i);
    }
    SessionId.UUIDFromBytes(uuidAsBytes);
    FlowType aFlow = topicDecoder.flow();
    String aTopic = topicDecoder.classification();
    if (topic.equals(aTopic)) {
      inboundFlow = aFlow;
      Topic readyTopic = SessionEventTopics.getTopic(MULTICAST_TOPIC, topic);
      reactor.post(readyTopic, buffer);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.fixp.Establisher#withOutboundKeepaliveInterval(int)
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
    this.topic = topic;
    return this;
  }
  
  void publishNewSession(ByteBuffer topicMessage) {
    Topic topic = SessionEventTopics.getTopic(NEW_SESSION_CREATED);
    reactor.post(topic, topicMessage);
  }

  /* (non-Javadoc)
   * @see io.fixprotocol.silverflash.Sender#send(java.nio.ByteBuffer)
   */
  @Override
  public long send(ByteBuffer message) throws IOException {
    return 0;
  }

  /* (non-Javadoc)
   * @see io.fixprotocol.silverflash.fixp.flow.FlowSender#sendHeartbeat()
   */
  @Override
  public void sendHeartbeat() throws IOException {
     
  }

  /* (non-Javadoc)
   * @see io.fixprotocol.silverflash.fixp.flow.FlowSender#sendEndOfStream()
   */
  @Override
  public void sendEndOfStream() throws IOException {

  }

}
