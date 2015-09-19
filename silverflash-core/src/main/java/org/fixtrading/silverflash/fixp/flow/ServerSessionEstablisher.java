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
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.NEW_SESSION_CREATED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.SERVER_ESTABLISHED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.SERVER_NEGOTIATED;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.fixtrading.silverflash.Sender;
import org.fixtrading.silverflash.fixp.Establisher;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.auth.AuthenticationClient;
import org.fixtrading.silverflash.fixp.auth.AuthenticationListener;
import org.fixtrading.silverflash.fixp.messages.EstablishmentReject;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.NegotiationReject;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.EstablishDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiateDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.EstablishmentAckEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.EstablishmentRejectEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.NegotiationRejectEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.NegotiationResponseEncoder;
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
  private FlowType inboundFlow;
  private int inboundKeepaliveInterval;
  private final MessageDecoder messageDecoder = new MessageDecoder();
  private final MessageEncoder messageEncoder = new MessageEncoder();
  private final FlowType outboundFlow;
  private int outboundKeepaliveInterval = DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL;
  private final EventReactor<ByteBuffer> reactor;
  private long requestTimestamp;
  private UUID sessionId;
  private final ByteBuffer sessionMessageBuffer = ByteBuffer.allocateDirect(128).order(
      ByteOrder.nativeOrder());
  private Topic terminatedTopic;
  private final Transport transport;
  private final byte[] uuidAsBytes = new byte[16];

  /**
   * Constructor
   * 
   * @param reactor an EventReactor
   * @param transport used to exchange messages with the client
   * @param outboundFlow client flow type
   */
  public ServerSessionEstablisher(EventReactor<ByteBuffer> reactor, Transport transport,
      FlowType outboundFlow) {
    Objects.requireNonNull(transport);
    this.reactor = reactor;
    this.transport = transport;
    this.outboundFlow = outboundFlow;
    this.authenticationClient = new AuthenticationClient(reactor);
  }

  @Override
  public void accept(ByteBuffer buffer) {
    try {
      Optional<Decoder> optDecoder = messageDecoder.attachForDecode(buffer, buffer.position());
      if (optDecoder.isPresent()) {
        final Decoder decoder = optDecoder.get();
        switch (decoder.getMessageType()) {
          case NEGOTIATE:
            onNegotiate((NegotiateDecoder) decoder);
            break;
          case ESTABLISH:
            onEstablish((EstablishDecoder) decoder);
            break;
          default:
            System.out
                .println("ServerSessionEstablisher: Protocol violation; unexpected session message");
        }
      } else {
        // Shouldn't get application message before handshake is done
        System.out
            .println("ServerSessionEstablisher: Protocol violation; unexpected application message");
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
      negotiationReject(requestTimestamp, NegotiationReject.CREDENTIALS);
    } catch (IOException e) {
      reactor.post(terminatedTopic, null);
    }
  }

  @Override
  public void connected() {
    // just wait for client to negotiate
  }

  public FlowType getInboundFlow() {
    return inboundFlow;
  }

  public int getInboundKeepaliveInterval() {
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

  void establishmentAck(long requestTimestamp, int keepaliveInterval) throws IOException {
    Topic readyTopic = SessionEventTopics.getTopic(sessionId, SERVER_ESTABLISHED);
    reactor.post(readyTopic, sessionMessageBuffer);

    sessionMessageBuffer.clear();
    EstablishmentAckEncoder establishEncoder =
        (EstablishmentAckEncoder) messageEncoder.attachForEncode(sessionMessageBuffer, 0,
            MessageType.ESTABLISHMENT_ACK);
    establishEncoder.setTimestamp(requestTimestamp);
    establishEncoder.setSessionId(uuidAsBytes);
    establishEncoder.setKeepaliveInterval(keepaliveInterval);
    // todo: retrieve persisted seqNo for recoverable flow
    establishEncoder.setNextSeqNoNull();
    send(sessionMessageBuffer);
    // System.out.println("Establishment ack sent");
  }

  void establishmentReject(long requestTimestamp, EstablishmentReject rejectCode)
      throws IOException {
    EstablishmentRejectEncoder establishEncoder =
        (EstablishmentRejectEncoder) messageEncoder.attachForEncode(sessionMessageBuffer, 0,
            MessageType.ESTABLISHMENT_REJECT);
    establishEncoder.setTimestamp(requestTimestamp);
    establishEncoder.setSessionId(uuidAsBytes);
    establishEncoder.setCode(rejectCode);
    establishEncoder.setReasonNull();
    send(sessionMessageBuffer);

    sessionMessageBuffer.rewind();
    reactor.post(terminatedTopic, sessionMessageBuffer);
  }

  void negotiationReject(long requestTimestamp, NegotiationReject rejectCode) throws IOException {
    NegotiationRejectEncoder negotiateEncoder =
        (NegotiationRejectEncoder) messageEncoder.attachForEncode(sessionMessageBuffer, 0,
            MessageType.NEGOTIATION_REJECT);
    negotiateEncoder.setRequestTimestamp(requestTimestamp);
    negotiateEncoder.setSessionId(uuidAsBytes);
    negotiateEncoder.setCode(rejectCode);
    // Maybe add reason text
    negotiateEncoder.setReasonNull();
    send(sessionMessageBuffer);

    sessionMessageBuffer.rewind();
    reactor.post(terminatedTopic, sessionMessageBuffer);
  }

  void negotiationResponse(long requestTimestamp, FlowType serverFlow) throws IOException {
    NegotiationResponseEncoder negotiateEncoder =
        (NegotiationResponseEncoder) messageEncoder.attachForEncode(sessionMessageBuffer, 0,
            MessageType.NEGOTIATION_RESPONSE);
    negotiateEncoder.setRequestTimestamp(requestTimestamp);
    negotiateEncoder.setSessionId(uuidAsBytes);
    negotiateEncoder.setServerFlow(serverFlow);
    send(sessionMessageBuffer);

    // Since subscription occurred before sessionId was available, use transport hashCode
    // as unique identifier.
    Topic initTopic = SessionEventTopics.getTopic(SERVER_NEGOTIATED, transport.hashCode());
    reactor.post(initTopic, sessionMessageBuffer);

    publishNewSession(sessionMessageBuffer.duplicate());
    // System.out.println("Negotiation response sent");
  }

  void onEstablish(EstablishDecoder establishDecoder) throws IOException {
    long requestTimestamp = establishDecoder.getRequestTimestamp();
    byte[] id = new byte[16];
    establishDecoder.getSessionId(id, 0);
    // Assuming that authentication is only done in Negotiate; may change
    if (Arrays.equals(uuidAsBytes, id)) {
      inboundKeepaliveInterval = establishDecoder.getKeepaliveInterval();
      establishDecoder.getNextSeqNo();
      establishmentAck(requestTimestamp, outboundKeepaliveInterval);
    } else {
      System.out.println("Unexpected establish received");
    }
  }

  void onNegotiate(NegotiateDecoder negotiateDecoder) {
    final ByteBuffer buffer = negotiateDecoder.getBuffer();

    requestTimestamp = negotiateDecoder.getTimestamp();
    negotiateDecoder.getSessionId(this.uuidAsBytes, 0);
    this.sessionId = SessionId.UUIDFromBytes(uuidAsBytes);

    inboundFlow = negotiateDecoder.getClientFlow();

    byte[] credentials = new byte[128];
    negotiateDecoder.getCredentials(credentials, 0);

    terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);

    authenticationClient.requestAuthentication(sessionId, buffer, this);
    // System.out.println("Negotiate received");
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

  void publishNewSession(ByteBuffer negotiationResponse) {
    Topic topic = SessionEventTopics.getTopic(NEW_SESSION_CREATED);
    reactor.post(topic, negotiationResponse);
  }
}
