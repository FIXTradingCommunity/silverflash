package org.fixtrading.silverflash.fixp.flow;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.HEARTBEAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.TerminationCode;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.TerminateEncoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.transport.Transport;

/**
 * Sends messages on an unsequenced flow.
 * 
 * @author Don Mendelson
 *
 */
public class UnsequencedFlowSender implements FlowSender {
  private final ByteBuffer heartbeatBuffer = ByteBuffer.allocateDirect(10).order(
      ByteOrder.nativeOrder());
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final MessageEncoder messageEncoder = new MessageEncoder();
  private final Transport transport;
  private final byte[] uuidAsBytes;

  private final Receiver heartbeatEvent = new Receiver() {

    public void accept(ByteBuffer t) {
      try {
        sendHeartbeat();
      } catch (IOException e) {
        Topic terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);
        reactor.post(terminatedTopic, t);
      }
    }

  };

  private final Subscription heartbeatSubscription;
  private final TimerSchedule heartbeatSchedule;
  private final UUID sessionId;
  private final EventReactor<ByteBuffer> reactor;

  public UnsequencedFlowSender(EventReactor<ByteBuffer> reactor, UUID sessionId,
      Transport transport, int outboundKeepaliveInterval) {
    Objects.requireNonNull(sessionId);
    Objects.requireNonNull(transport);
    this.reactor = reactor;
    this.sessionId = sessionId;
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
    this.transport = transport;
    messageEncoder.attachForEncode(heartbeatBuffer, 0, MessageType.UNSEQUENCED_HEARTBEAT);

    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule =
        reactor.postAtInterval(heartbeatTopic, heartbeatBuffer, outboundKeepaliveInterval);
  }

  @Override
  public long send(ByteBuffer message) throws IOException {
    Objects.requireNonNull(message);
    transport.write(message);
    isHeartbeatDue.set(false);
    return 0;
  }

  @Override
  public long send(ByteBuffer[] messages) throws IOException {
    Objects.requireNonNull(messages);
    transport.write(messages);
    isHeartbeatDue.set(false);
    return 0;
  }

  public void sendEndOfStream() throws IOException {
    heartbeatSchedule.cancel();
    heartbeatSubscription.unsubscribe();

    final ByteBuffer terminateBuffer = ByteBuffer.allocateDirect(29).order(ByteOrder.nativeOrder());
    TerminateEncoder terminateEncoder =
        (TerminateEncoder) messageEncoder
            .attachForEncode(terminateBuffer, 0, MessageType.TERMINATE);
    terminateEncoder.setSessionId(uuidAsBytes);
    terminateEncoder.setCode(TerminationCode.FINISHED);
    terminateEncoder.setReasonNull();
    transport.write(terminateBuffer);
  }

  public void sendHeartbeat() throws IOException {
    if (isHeartbeatDue.getAndSet(true)) {
      transport.write(heartbeatBuffer);
    }
  }
}
