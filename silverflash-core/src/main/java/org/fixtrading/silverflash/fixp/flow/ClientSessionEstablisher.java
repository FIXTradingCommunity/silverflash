package org.fixtrading.silverflash.fixp.flow;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.*;

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
import org.fixtrading.silverflash.fixp.messages.EstablishmentReject;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.NegotiationReject;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.EstablishmentAckDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.EstablishmentRejectDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiationRejectDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiationResponseDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.EstablishEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.NegotiateEncoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.transport.Transport;

/**
 * Implements the FIXP protocol client handshake
 * 
 * @author Don Mendelson
 *
 */
public class ClientSessionEstablisher implements Sender, Establisher, FlowReceiver, FlowSender {
  /**
	 * 
	 */
  public static final int DEFAULT_OUTBOUND_KEEPALIVE_INTERVAL = 5000;
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
  private byte[] uuidAsBytes;
  private byte[] credentials;

  /**
   * Constructor
   * 
   * @param reactor an EventReactor
   * @param outboundFlow client flow type
   * @param transport used to exchange messages with the server
   */
  public ClientSessionEstablisher(EventReactor<ByteBuffer> reactor, FlowType outboundFlow,
      Transport transport) {
    this.reactor = reactor;
    this.outboundFlow = outboundFlow;
    this.transport = transport;
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

  @Override
  public void accept(ByteBuffer buffer) {
    try {
      Optional<Decoder> optDecoder = messageDecoder.attachForDecode(buffer, buffer.position());
      if (optDecoder.isPresent()) {
        final Decoder decoder = optDecoder.get();
        switch (decoder.getMessageType()) {
          case NEGOTIATION_RESPONSE:
            onNegotiationResponse((NegotiationResponseDecoder) decoder);
            break;
          case NEGOTIATION_REJECT:
            onNegotiationReject((NegotiationRejectDecoder) decoder);
            break;
          case ESTABLISHMENT_ACK:
            onEstablishmentAck((EstablishmentAckDecoder) decoder);
            break;
          case ESTABLISHMENT_REJECT:
            onEstablishmentReject((EstablishmentRejectDecoder) decoder);
            break;
          default:
            System.out.println("Protocol violation");
        }
      } else {
        // Shouldn't get application message before handshake is done
        System.out.println("Protocol violation");
      }
    } catch (IOException ex) {

    }
  }

  @Override
  public void connected() {
    try {
      negotiate();
    } catch (IOException e) {
      reactor.post(terminatedTopic, null);
    }
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

  public byte[] getSessionId() {
    return uuidAsBytes;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.flow.FlowReceiver#isHeartbeatDue()
   */
  public boolean isHeartbeatDue() {
    return false;
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
   * Client function
   * 
   * @param keepaliveInterval heartbeat interval
   * @throws IOException if message cannot be sent
   */
  void establish(int keepaliveInterval) throws IOException {
    sessionMessageBuffer.clear();
    EstablishEncoder establishEncoder =
        (EstablishEncoder) messageEncoder.attachForEncode(sessionMessageBuffer, 0,
            MessageType.ESTABLISH);
    requestTimestamp = System.nanoTime();
    establishEncoder.setTimestamp(requestTimestamp);
    establishEncoder.setSessionId(uuidAsBytes);
    establishEncoder.setKeepaliveInterval(keepaliveInterval);
    // todo: retrieve persisted seqNo for recoverable flow
    establishEncoder.setNextSeqNoNull();
    // Assuming that authentication is only done in Negotiate; may change
    establishEncoder.setCredentialsNull();
    send(sessionMessageBuffer);
    // System.out.println("Establish sent");
  }

  /**
   * Negotiate with server to create a new session
   * 
   * @throws IOException if a message cannot be sent
   */
  void negotiate() throws IOException {
    NegotiateEncoder negotiateEncoder =
        (NegotiateEncoder) messageEncoder.attachForEncode(sessionMessageBuffer, 0,
            MessageType.NEGOTIATE);
    requestTimestamp = System.nanoTime();
    negotiateEncoder.setTimestamp(requestTimestamp);
    negotiateEncoder.setSessionId(uuidAsBytes);
    negotiateEncoder.setClientFlow(outboundFlow);
    negotiateEncoder.setCredentials(credentials);
    long bytesWritten = send(sessionMessageBuffer);
  }

  void onEstablishmentAck(EstablishmentAckDecoder establishDecoder) {
    final ByteBuffer buffer = establishDecoder.getBuffer();
    buffer.mark();
    long receivedTimestamp = establishDecoder.getRequestTimestamp();
    byte[] id = new byte[16];
    establishDecoder.getSessionId(id, 0);
    this.inboundKeepaliveInterval = establishDecoder.getKeepaliveInterval();
    // todo: retrieve persisted seqNo for recoverable flow
    long nextSeqNo = establishDecoder.getNextSeqNo();

    if (receivedTimestamp == requestTimestamp && Arrays.equals(id, uuidAsBytes)) {

      Topic readyTopic = SessionEventTopics.getTopic(sessionId, CLIENT_ESTABLISHED);
      buffer.reset();
      reactor.post(readyTopic, buffer);
      // System.out.println("Establishment ack received");
    } else {
      System.out.println("Unexpected establishment ack received");
    }
  }

  void onEstablishmentReject(EstablishmentRejectDecoder establishDecoder) {
    ByteBuffer buffer = establishDecoder.getBuffer();
    buffer.mark();
    requestTimestamp = establishDecoder.getRequestTimestamp();
    byte[] id = new byte[16];
    establishDecoder.getSessionId(id, 0);
    EstablishmentReject rejectCode = establishDecoder.getCode();

    buffer.reset();
    reactor.post(terminatedTopic, buffer);
  }

  void onNegotiationReject(NegotiationRejectDecoder negotiateDecoder) {
    final ByteBuffer buffer = negotiateDecoder.getBuffer();
    buffer.mark();
    long receivedTimestamp = negotiateDecoder.getRequestTimestamp();
    byte[] id = new byte[16];
    negotiateDecoder.getSessionId(id, 0);
    if (receivedTimestamp == requestTimestamp && Arrays.equals(id, uuidAsBytes)) {
      NegotiationReject rejectCode = negotiateDecoder.getCode();

      buffer.reset();
      reactor.post(terminatedTopic, buffer);
    }
  }

  void onNegotiationResponse(NegotiationResponseDecoder negotiateDecoder) throws IOException {
    long receivedTimestamp = negotiateDecoder.getRequestTimestamp();
    byte[] id = new byte[16];
    negotiateDecoder.getSessionId(id, 0);

    if (receivedTimestamp == requestTimestamp && Arrays.equals(id, uuidAsBytes)) {
      inboundFlow = negotiateDecoder.getServerFlow();
      establish(outboundKeepaliveInterval);
    } else {
      System.out.println("Unexpected negotiation response received");
    }
  }
}
