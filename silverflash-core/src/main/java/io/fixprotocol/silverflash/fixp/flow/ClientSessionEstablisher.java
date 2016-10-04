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

import static io.fixprotocol.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.SessionEventType.CLIENT_ESTABLISHED;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.Sender;
import io.fixprotocol.silverflash.fixp.Establisher;
import io.fixprotocol.silverflash.fixp.SessionEventTopics;
import io.fixprotocol.silverflash.fixp.SessionId;
import io.fixprotocol.silverflash.fixp.messages.EstablishEncoder;
import io.fixprotocol.silverflash.fixp.messages.EstablishmentAckDecoder;
import io.fixprotocol.silverflash.fixp.messages.EstablishmentRejectCode;
import io.fixprotocol.silverflash.fixp.messages.EstablishmentRejectDecoder;
import io.fixprotocol.silverflash.fixp.messages.FlowType;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderDecoder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.fixp.messages.NegotiateEncoder;
import io.fixprotocol.silverflash.fixp.messages.NegotiationRejectCode;
import io.fixprotocol.silverflash.fixp.messages.NegotiationRejectDecoder;
import io.fixprotocol.silverflash.fixp.messages.NegotiationResponseDecoder;
import io.fixprotocol.silverflash.frame.MessageFrameEncoder;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.reactor.Topic;
import io.fixprotocol.silverflash.transport.Transport;

/**
 * Implements the FIXP protocol client handshake
 * 
 * @author Don Mendelson
 *
 */
public class ClientSessionEstablisher implements Sender, Establisher, FlowReceiver, FlowSender {

  public static final int DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL = 5000;
  private byte[] credentials;
  private EstablishEncoder establishEncoder = new EstablishEncoder();
  private final EstablishmentAckDecoder establishmentAckDecoder = new EstablishmentAckDecoder();

  private final EstablishmentRejectDecoder establishmentRejectDecoder = new EstablishmentRejectDecoder();
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
  private FlowType inboundFlow;
  private long inboundKeepaliveInterval;
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final NegotiateEncoder negotiateEncoder = new NegotiateEncoder();
  private final NegotiationRejectDecoder negotiationRejectDecoder = new NegotiationRejectDecoder();
  private final NegotiationResponseDecoder negotiationResponseDecoder = new NegotiationResponseDecoder();
  private final FlowType outboundFlow;
  private long outboundKeepaliveInterval = DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL;
  private final EventReactor<ByteBuffer> reactor;
  private long requestTimestamp;
  private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(128).order(
      ByteOrder.nativeOrder());
  private UUID sessionId;
  private Topic terminatedTopic;
  private final Transport transport;
  private byte[] uuidAsBytes;
  private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);
  private final MessageFrameEncoder frameEncoder;

  
  /**
   * Constructor
   * 
   * @param frameEncoder delimits messages
   * @param reactor an EventReactor
   * @param outboundFlow client flow type
   * @param transport used to exchange messages with the server
   */
  public ClientSessionEstablisher(MessageFrameEncoder frameEncoder, EventReactor<ByteBuffer> reactor, FlowType outboundFlow,
      Transport transport) {
    this.frameEncoder = frameEncoder;
    this.reactor = reactor;
    this.outboundFlow = outboundFlow;
    this.transport = transport;
  }

  @Override
  public void accept(ByteBuffer buffer) {
    try {
      immutableBuffer.wrap(buffer);
      int offset = buffer.position();
      messageHeaderDecoder.wrap(immutableBuffer, offset);
      offset += messageHeaderDecoder.encodedLength();
      if (messageHeaderDecoder.schemaId() == negotiationResponseDecoder.sbeSchemaId()) {

        switch (messageHeaderDecoder.templateId()) {
         case NegotiationResponseDecoder.TEMPLATE_ID:
           negotiationResponseDecoder.wrap(immutableBuffer, offset,
               negotiationResponseDecoder.sbeBlockLength(), negotiationResponseDecoder.sbeSchemaVersion());
          onNegotiationResponse(negotiationResponseDecoder);
          break;
        case NegotiationRejectDecoder.TEMPLATE_ID:
          negotiationRejectDecoder.wrap(immutableBuffer, offset,
              negotiationRejectDecoder.sbeBlockLength(), negotiationRejectDecoder.sbeSchemaVersion());
          onNegotiationReject(negotiationRejectDecoder, buffer);
          break;
        case EstablishmentAckDecoder.TEMPLATE_ID:
          establishmentAckDecoder.wrap(immutableBuffer, offset,
              establishmentAckDecoder.sbeBlockLength(), establishmentAckDecoder.sbeSchemaVersion());
          onEstablishmentAck(establishmentAckDecoder, buffer);
          break;
        case EstablishmentRejectDecoder.TEMPLATE_ID:
          establishmentRejectDecoder.wrap(immutableBuffer, offset,
              establishmentRejectDecoder.sbeBlockLength(), establishmentRejectDecoder.sbeSchemaVersion());
          onEstablishmentReject(establishmentRejectDecoder, buffer);
          break;
        default:
//          System.out
//              .println("ClientSessionEstablisher: Protocol violation; unexpected session message "
//                  + decoder.getMessageType());
          reactor.post(terminatedTopic, buffer);
        }
      } else {
        // Shouldn't get application message before handshake is done
        // System.out.println("Protocol violation");
        reactor.post(terminatedTopic, buffer);
      }
    } catch (IOException ex) {

    }
  }


  /* (non-Javadoc)
   * @see io.fixprotocol.silverflash.fixp.Establisher#complete()
   */
  @Override
  public void complete() {
    
  }

  @Override
  public void connected() {
    try {
      negotiate();
    } catch (IOException e) {
      reactor.post(terminatedTopic, null);
    }
  }

  /**
   * Client function
   * 
   * @param outboundKeepaliveInterval heartbeat interval
   * @throws IOException if message cannot be sent
   */
  void establish(long outboundKeepaliveInterval) throws IOException {
    sendBuffer.clear();
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(establishEncoder.sbeBlockLength())
        .templateId(establishEncoder.sbeTemplateId()).schemaId(establishEncoder.sbeSchemaId())
        .version(establishEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    establishEncoder.wrap(mutableBuffer, offset);

    requestTimestamp = System.nanoTime();
    establishEncoder.timestamp(requestTimestamp);
    for (int i = 0; i < 16; i++) {
      establishEncoder.sessionId(i, uuidAsBytes[i]);
    }

    establishEncoder.keepaliveInterval(outboundKeepaliveInterval);
    frameEncoder.setMessageLength(offset + establishEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
    // todo: retrieve persisted seqNo for recoverable flow
    // establishEncoder.nextSeqNo();
    // Assuming that authentication is only done in Negotiate; may change
    // establishEncoder.credentials();
    send(sendBuffer);
    // System.out.println("Establish sent");
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

  public byte[] getSessionId() {
    return uuidAsBytes;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.fixp.flow.FlowReceiver#isHeartbeatDue()
   */
  public boolean isHeartbeatDue() {
    return false;
  }

  /**
   * Negotiate with server to create a new session
   * 
   * @throws IOException if a message cannot be sent
   */
  void negotiate() throws IOException {
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(negotiateEncoder.sbeBlockLength())
        .templateId(negotiateEncoder.sbeTemplateId()).schemaId(negotiateEncoder.sbeSchemaId())
        .version(negotiateEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    negotiateEncoder.wrap(mutableBuffer, offset);
    requestTimestamp = System.nanoTime();
    negotiateEncoder.timestamp(requestTimestamp);
    for (int i = 0; i < 16; i++) {
      negotiateEncoder.sessionId(i, uuidAsBytes[i]);
    }
    negotiateEncoder.clientFlow(outboundFlow);
    negotiateEncoder.putCredentials(credentials, 0, credentials.length);
    frameEncoder.setMessageLength(offset + negotiateEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();

    long bytesWritten = send(sendBuffer);
  }

  void onEstablishmentAck(EstablishmentAckDecoder establishDecoder, ByteBuffer buffer) {
    long receivedTimestamp = establishDecoder.requestTimestamp();
    byte[] id = new byte[16];
    for (int i = 0; i < 16; i++) {
      id[i] = (byte) establishDecoder.sessionId(i);
    }

    this.inboundKeepaliveInterval = establishDecoder.keepaliveInterval();
    // todo: retrieve persisted seqNo for recoverable flow
    long nextSeqNo = establishDecoder.nextSeqNo();

    if (receivedTimestamp == requestTimestamp && Arrays.equals(id, uuidAsBytes)) {

      Topic readyTopic = SessionEventTopics.getTopic(sessionId, CLIENT_ESTABLISHED);
      reactor.post(readyTopic, buffer);
      // System.out.println("Establishment ack received");
    } else {
      System.err.println("ClientSessionEstablisher: Unexpected establishment ack received");
    }
  }

  void onEstablishmentReject(EstablishmentRejectDecoder establishDecoder, ByteBuffer buffer) {
    requestTimestamp = establishDecoder.requestTimestamp();
    byte[] id = new byte[16];
    for (int i = 0; i < 16; i++) {
      id[i] = (byte) establishDecoder.sessionId(i);
    }

    EstablishmentRejectCode rejectCode = establishDecoder.code();

    reactor.post(terminatedTopic, buffer);
  }

  void onNegotiationReject(NegotiationRejectDecoder negotiateDecoder, ByteBuffer buffer) {
    long receivedTimestamp = negotiateDecoder.requestTimestamp();
    byte[] id = new byte[16];
    for (int i = 0; i < 16; i++) {
      id[i] = (byte) negotiateDecoder.sessionId(i);
    }

    if (receivedTimestamp == requestTimestamp && Arrays.equals(id, uuidAsBytes)) {
      NegotiationRejectCode rejectCode = negotiateDecoder.code();
      reactor.post(terminatedTopic, buffer);
    }
  }

  void onNegotiationResponse(NegotiationResponseDecoder negotiateDecoder) throws IOException {
    long receivedTimestamp = negotiateDecoder.requestTimestamp();
    byte[] id = new byte[16];
    for (int i = 0; i < 16; i++) {
      id[i] = (byte) negotiateDecoder.sessionId(i);
    }

    if (receivedTimestamp == requestTimestamp && Arrays.equals(id, uuidAsBytes)) {
      inboundFlow = negotiateDecoder.serverFlow();
      establish(outboundKeepaliveInterval);
    } else {
      System.out.println("Unexpected negotiation response received");
    }
  }

  @Override
  public long send(ByteBuffer message) throws IOException {
    Objects.requireNonNull(message);
    return transport.write(message);
  }

  public void sendEndOfStream() throws IOException {

  }

  public void sendHeartbeat() throws IOException {

  }

  /**
   * Sets the credentials for session negotiation
   * 
   * @param sessionId unique session identifier
   * @param credentials business entity identification
   * @return this ClientSessionEstablisher
   */
  public ClientSessionEstablisher withCredentials(UUID sessionId, byte[] credentials) {
    Objects.requireNonNull(sessionId);
    Objects.requireNonNull(credentials);
    this.sessionId = sessionId;
    this.credentials = credentials;
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
    terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);
    return this;
  }

  /**
   * Sets the heartbeat interval
   */
  public ClientSessionEstablisher withOutboundKeepaliveInterval(int outboundKeepaliveInterval) {
    this.outboundKeepaliveInterval = outboundKeepaliveInterval;
    return this;
  }
}
