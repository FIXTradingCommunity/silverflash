/**
 * Copyright 2015-2016 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.fixprotocol.silverflash.fixp.flow;

import static io.fixprotocol.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.SessionEventType.MULTICAST_TOPIC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.UUID;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.Sender;
import io.fixprotocol.silverflash.fixp.Establisher;
import io.fixprotocol.silverflash.fixp.SessionEventTopics;
import io.fixprotocol.silverflash.fixp.SessionId;
import io.fixprotocol.silverflash.fixp.messages.FlowType;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.fixp.messages.TopicEncoder;
import io.fixprotocol.silverflash.frame.MessageFrameEncoder;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.reactor.Topic;
import io.fixprotocol.silverflash.transport.Transport;

/**
 * Implements the FIXP protocol server handshake
 * 
 * @author Don Mendelson
 *
 */
public class MulticastProducerEstablisher implements Sender, Establisher, FlowReceiver, FlowSender {
  public static final long DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL = 5000;

  private final FlowType outboundFlow;
  private long outboundKeepaliveInterval = DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL;
  private final EventReactor<ByteBuffer> reactor;
  private final ByteBuffer topicBuffer =
      ByteBuffer.allocateDirect(128).order(ByteOrder.nativeOrder());
  private final MutableDirectBuffer directBuffer = new UnsafeBuffer(topicBuffer);
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final Topic terminatedTopic;
  private final Transport transport;
  private byte[] uuidAsBytes = new byte[16];
  private final String topic;
  private final TopicEncoder topicEncoder = new TopicEncoder();
  private final MessageFrameEncoder frameEncoder;

  /**
   * Constructor
   * 
   * @param frameEncoder delimits messages
   * @param reactor an EventReactor
   * @param transport used to exchange messages with the client
   * @param outboundFlow client flow type
   * @param topic FIXP topic to publish
   * @param sessionId session ID for the topic
   */
  public MulticastProducerEstablisher(MessageFrameEncoder frameEncoder, EventReactor<ByteBuffer> reactor, Transport transport,
      FlowType outboundFlow, String topic, UUID sessionId) {
    Objects.requireNonNull(transport);
    Objects.requireNonNull(topic);
    this.frameEncoder = frameEncoder;
    this.reactor = reactor;
    this.transport = transport;
    this.outboundFlow = outboundFlow;
    this.topic = topic;
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
    terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);
  }

  @Override
  public void accept(ByteBuffer buffer) {}

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
    return FlowType.None;
  }

  public long getInboundKeepaliveInterval() {
    return 0;
  }

  public FlowType getOutboundFlow() {
    return outboundFlow;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.Establisher#getOutboundKeepaliveInterval()
   */
  public long getOutboundKeepaliveInterval() {
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
    int offset = 0;
    frameEncoder.wrap(topicBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(directBuffer, offset);
    messageHeaderEncoder.blockLength(topicEncoder.sbeBlockLength())
        .templateId(topicEncoder.sbeTemplateId()).schemaId(topicEncoder.sbeSchemaId())
        .version(topicEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    topicEncoder.wrap(directBuffer, offset);
    for (int i = 0; i < 16; i++) {
      topicEncoder.sessionId(i, uuidAsBytes[i]);
    }

    topicEncoder.flow(outboundFlow);
    topicEncoder.classification(topic);
    frameEncoder.setMessageLength(offset + topicEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
    send(topicBuffer);

    Topic initTopic = SessionEventTopics.getTopic(MULTICAST_TOPIC, topic);
    reactor.post(initTopic, topicBuffer);
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.fixp.Establisher#withOutboundKeepaliveInterval(int)
   */
  public MulticastProducerEstablisher withOutboundKeepaliveInterval(int outboundKeepaliveInterval) {
    this.outboundKeepaliveInterval = outboundKeepaliveInterval;
    return this;
  }

}
