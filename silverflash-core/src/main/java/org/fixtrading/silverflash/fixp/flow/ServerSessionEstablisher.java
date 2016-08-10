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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.NEW_SESSION_CREATED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.SERVER_ESTABLISHED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.SERVER_NEGOTIATED;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.fixtrading.silverflash.Sender;
import org.fixtrading.silverflash.fixp.Establisher;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.auth.AuthenticationClient;
import org.fixtrading.silverflash.fixp.auth.AuthenticationListener;
import org.fixtrading.silverflash.fixp.messages.EstablishDecoder;
import org.fixtrading.silverflash.fixp.messages.EstablishmentAckEncoder;
import org.fixtrading.silverflash.fixp.messages.EstablishmentRejectCode;
import org.fixtrading.silverflash.fixp.messages.EstablishmentRejectEncoder;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.NegotiateDecoder;
import org.fixtrading.silverflash.fixp.messages.NegotiationRejectCode;
import org.fixtrading.silverflash.fixp.messages.NegotiationRejectEncoder;
import org.fixtrading.silverflash.fixp.messages.NegotiationResponseEncoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.transport.Transport;

/**
 * Implements the FIXP protocol server handshake
 * 
 * @author Don Mendelson
 *
 */
public class ServerSessionEstablisher implements Sender, Establisher, FlowReceiver, FlowSender,
    AuthenticationListener {
  public static final int DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL = 5000;

  private final AuthenticationClient authenticationClient;
  private final EstablishDecoder establishDecoder = new EstablishDecoder();
  private final EstablishmentAckEncoder establishmentAckEncoder = new EstablishmentAckEncoder();
  private final EstablishmentRejectEncoder establishmentRejectEncoder = new EstablishmentRejectEncoder();
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
  private FlowType inboundFlow;
  private long inboundKeepaliveInterval;
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final NegotiateDecoder negotiateDecoder = new NegotiateDecoder();
  private final NegotiationRejectEncoder negotiationRejectEncoder = new NegotiationRejectEncoder();
  private final NegotiationResponseEncoder negotiationResponseEncoder = new NegotiationResponseEncoder();
  private final FlowType outboundFlow;
  private int outboundKeepaliveInterval = DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL;
  private final EventReactor<ByteBuffer> reactor;
  private long requestTimestamp;
  private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(128).order(
      ByteOrder.nativeOrder());
  private UUID sessionId;
  private Topic terminatedTopic;
  private final Transport transport;
  private final byte[] uuidAsBytes = new byte[16];
  private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);
  private final MessageFrameEncoder frameEncoder;

  
  /**
   * Constructor
   * 
   * @param reactor an EventReactor
   * @param transport used to exchange messages with the client
   * @param outboundFlow client flow type
   */
  public ServerSessionEstablisher(MessageFrameEncoder frameEncoder, EventReactor<ByteBuffer> reactor, Transport transport,
      FlowType outboundFlow) {
    Objects.requireNonNull(transport);
    this.frameEncoder = frameEncoder;
    this.reactor = reactor;
    this.transport = transport;
    this.outboundFlow = outboundFlow;
    this.authenticationClient = new AuthenticationClient(reactor);
  }

  @Override
  public void accept(ByteBuffer buffer) {
    try {
      immutableBuffer.wrap(buffer);
      int offset = buffer.position();
      messageHeaderDecoder.wrap(immutableBuffer, offset);
      offset += messageHeaderDecoder.encodedLength();

      if (messageHeaderDecoder.schemaId() == negotiateDecoder.sbeSchemaId()) {

        switch (messageHeaderDecoder.templateId()) {
        case NegotiateDecoder.TEMPLATE_ID:
          negotiateDecoder.wrap(immutableBuffer, offset,
              negotiateDecoder.sbeBlockLength(), negotiateDecoder.sbeSchemaVersion());
          onNegotiate(negotiateDecoder, buffer);
          break;
        case EstablishDecoder.TEMPLATE_ID:
          establishDecoder.wrap(immutableBuffer, offset,
              establishDecoder.sbeBlockLength(), establishDecoder.sbeSchemaVersion());
          onEstablish(establishDecoder, buffer);
          break;
        default:
//          System.out
//              .println("ServerSessionEstablisher: Protocol violation; unexpected session message "
//                  + decoder.getMessageType());
          if (terminatedTopic != null) {
             reactor.post(terminatedTopic, buffer);
          }
        }
      } else {
        // Shouldn't get application message before handshake is done
        System.out.println(
            "ServerSessionEstablisher: Protocol violation; unexpected application message");
      }
    } catch (IOException ex) {

    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.auth.AuthenticationListener#authenticated(byte[])
   */
  public void authenticated(UUID uuid) {
    try {
      negotiationResponse(requestTimestamp, outboundFlow);
    } catch (IOException e) {
      reactor.post(terminatedTopic, null);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.auth.AuthenticationListener#authenticationFailed
   * (java.util.UUID)
   */
  public void authenticationFailed(UUID sessionId) {
    try {
      negotiationReject(requestTimestamp, NegotiationRejectCode.Credentials);
    } catch (IOException e) {
      reactor.post(terminatedTopic, null);
    }
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.fixp.Establisher#complete()
   */
  @Override
  public void complete() throws IOException {
      establishmentAck(requestTimestamp, outboundKeepaliveInterval);
  }

  @Override
  public void connected() {
    // just wait for client to negotiate
  }

  private ByteBuffer copyBuffer(final ByteBuffer src, int offset) {
    ByteBuffer dest = ByteBuffer.allocate(src.limit()-offset);
    src.position(offset);
    dest.put(src);
    return dest;
  }

  void establishmentAck(long requestTimestamp, int keepaliveInterval) throws IOException {
    sendBuffer.clear();
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(establishmentAckEncoder.sbeBlockLength())
        .templateId(establishmentAckEncoder.sbeTemplateId()).schemaId(establishmentAckEncoder.sbeSchemaId())
        .version(establishmentAckEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    establishmentAckEncoder.wrap(mutableBuffer, offset);

    establishmentAckEncoder.requestTimestamp(requestTimestamp);
    for (int i = 0; i < 16; i++) {
      establishmentAckEncoder.sessionId(i, uuidAsBytes[i]);
    }
    establishmentAckEncoder.keepaliveInterval(keepaliveInterval);
    // todo: retrieve persisted seqNo for recoverable flow
    establishmentAckEncoder.nextSeqNo(EstablishmentAckEncoder.nextSeqNoNullValue());
    frameEncoder.setMessageLength(offset + establishmentAckEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
    
    send(sendBuffer);
    // System.out.println("Establishment ack sent");
  }

  void establishmentReject(long requestTimestamp, EstablishmentRejectCode rejectCode)
      throws IOException {
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(establishmentRejectEncoder.sbeBlockLength())
        .templateId(establishmentRejectEncoder.sbeTemplateId()).schemaId(establishmentRejectEncoder.sbeSchemaId())
        .version(establishmentRejectEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    establishmentRejectEncoder.wrap(mutableBuffer, offset);

    establishmentRejectEncoder.requestTimestamp(requestTimestamp);
    for (int i = 0; i < 16; i++) {
      establishmentRejectEncoder.sessionId(i, uuidAsBytes[i]);
    }
    establishmentRejectEncoder.code(rejectCode);
    //establishmentRejectEncoder.reason();
    frameEncoder.setMessageLength(offset + establishmentRejectEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
    send(sendBuffer);

    sendBuffer.rewind();
    reactor.post(terminatedTopic, sendBuffer);
  }

  public FlowType getInboundFlow() {
    return inboundFlow;
  }

  public long getInboundKeepaliveInterval() {
    return inboundKeepaliveInterval;
  }

  public FlowType getOutboundFlow() {
    return outboundFlow;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Establisher#getOutboundKeepaliveInterval()
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

  void negotiationReject(long requestTimestamp, NegotiationRejectCode rejectCode) throws IOException {
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(negotiationRejectEncoder.sbeBlockLength())
        .templateId(negotiationRejectEncoder.sbeTemplateId()).schemaId(negotiationRejectEncoder.sbeSchemaId())
        .version(negotiationRejectEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    negotiationRejectEncoder.wrap(mutableBuffer, offset);
    negotiationRejectEncoder.requestTimestamp(requestTimestamp);
    for (int i = 0; i < 16; i++) {
      negotiationRejectEncoder.sessionId(i, uuidAsBytes[i]);
    }
    negotiationRejectEncoder.code(rejectCode);
    // Maybe add reason text
    // negotiationRejectEncoder.reason();
    frameEncoder.setMessageLength(offset + negotiationRejectEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
    send(sendBuffer);

    sendBuffer.rewind();
    reactor.post(terminatedTopic, sendBuffer);
  }

  void negotiationResponse(long requestTimestamp, FlowType serverFlow) throws IOException {
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(negotiationResponseEncoder.sbeBlockLength())
        .templateId(negotiationResponseEncoder.sbeTemplateId()).schemaId(negotiationResponseEncoder.sbeSchemaId())
        .version(negotiationResponseEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    negotiationResponseEncoder.wrap(mutableBuffer, offset);
    negotiationResponseEncoder.requestTimestamp(requestTimestamp);
    for (int i = 0; i < 16; i++) {
      negotiationResponseEncoder.sessionId(i, uuidAsBytes[i]);
    }
    negotiationResponseEncoder.serverFlow(serverFlow);
    frameEncoder.setMessageLength(offset + negotiationResponseEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
    send(sendBuffer);

     // Since subscription occurred before sessionId was available, use transport hashCode
    // as unique identifier.
    Topic initTopic = SessionEventTopics.getTopic(SERVER_NEGOTIATED, transport.hashCode());
    reactor.post(initTopic, sendBuffer);

    publishNewSession(sendBuffer.duplicate());
    // System.out.println("Negotiation response sent");
  }

  void onEstablish(EstablishDecoder establishDecoder, ByteBuffer buffer) throws IOException {
    byte[] id = new byte[16];
    for (int i = 0; i < 16; i++) {
      id[i] = (byte) establishDecoder.sessionId(i);
    }
    // Assuming that authentication is only done in Negotiate; may change
    if (Arrays.equals(uuidAsBytes, id)) {
      this.requestTimestamp = establishDecoder.timestamp();
      inboundKeepaliveInterval = establishDecoder.keepaliveInterval();
      long inboundNextSeqNo = establishDecoder.nextSeqNo();

      Topic readyTopic = SessionEventTopics.getTopic(sessionId, SERVER_ESTABLISHED);
      reactor.post(readyTopic, sendBuffer);
    } else {
      // System.out.println("Unexpected establish received");
      reactor.post(terminatedTopic, buffer);
    }
  }

  void onNegotiate(NegotiateDecoder negotiateDecoder, ByteBuffer buffer) {
    int pos = buffer.position();
    
    requestTimestamp = negotiateDecoder.timestamp();
    for (int i = 0; i < 16; i++) {
      uuidAsBytes[i] = (byte) negotiateDecoder.sessionId(i);
    }
    this.sessionId = SessionId.UUIDFromBytes(uuidAsBytes);

    inboundFlow = negotiateDecoder.clientFlow();

    terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);

    // duplicate buffer for async behavior (deep copy)
    final ByteBuffer toAuthenticator = copyBuffer(buffer, pos);
    
    authenticationClient.requestAuthentication(sessionId, toAuthenticator, this);
    // System.out.println("Negotiate received");
  }

  void publishNewSession(ByteBuffer negotiationResponse) {
    Topic topic = SessionEventTopics.getTopic(NEW_SESSION_CREATED);
    reactor.post(topic, negotiationResponse);
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

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.Establisher#withOutboundKeepaliveInterval(int)
   */
  public ServerSessionEstablisher withOutboundKeepaliveInterval(int outboundKeepaliveInterval) {
    this.outboundKeepaliveInterval = outboundKeepaliveInterval;
    return this;
  }
}
