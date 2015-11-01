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
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.HEARTBEAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.RecoverableSender;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.FinishedSendingEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.RetransmissionEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.store.MessageStore;
import org.fixtrading.silverflash.fixp.store.StoreException;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * Sends messages on an recoverable flow on a Transport that guarantees FIFO delivery. The
 * implementation sends a Sequence message only at startup, when context changes between
 * retransmission and real-time messages, and for heartbeats.
 * 
 * @author Don Mendelson
 *
 */
@SuppressWarnings("unchecked")
public class RecoverableFlowSender extends AbstractFlow
    implements RecoverableSender, MutableSequence {

  @SuppressWarnings("rawtypes")
  public static class Builder<T extends RecoverableFlowSender, B extends FlowBuilder>
      extends AbstractFlow.Builder {

    private MessageStore store;

    public T build() {
      return (T) new RecoverableFlowSender(this);
    }

    public B withMessageStore(MessageStore store) {
      this.store = store;
      return (B) this;
    }
  };

  @SuppressWarnings("rawtypes")
  public static Builder builder() {
    return new Builder();
  }

  private final AtomicBoolean criticalSection = new AtomicBoolean();
  private ByteBuffer finishedBuffer;
  private ByteBuffer heartbeatBuffer;
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
  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final AtomicBoolean isRetransmission = new AtomicBoolean();
  private final ByteBuffer[] one = new ByteBuffer[1];
  private final ByteBuffer retransBuffer = ByteBuffer.allocateDirect(46)
      .order(ByteOrder.nativeOrder());
  private final RetransmissionEncoder retransmissionEncoder;
  private final ByteBuffer[] srcs = new ByteBuffer[32];

  private final MessageStore store;

  protected RecoverableFlowSender(Builder builder) {
    super(builder);
    Objects.requireNonNull(builder.store);
    this.store = builder.store;
    retransmissionEncoder = (RetransmissionEncoder) messageEncoder.wrap(retransBuffer, 0,
        MessageType.RETRANSMISSION);
    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, ByteBuffer.allocate(0),
        keepaliveInterval);
  }

  public long getNextSeqNo() {
    return sequencer.getNextSeqNo();
  }

  private void persist(long seqNo, ByteBuffer message) throws StoreException {
    store.insertMessage(sessionId, seqNo, message);
  }

  @Override
  public void resend(ByteBuffer message, long seqNo, long requestTimestamp) throws IOException {
    Objects.requireNonNull(message);
    while (!criticalSection.compareAndSet(false, true)) {
      Thread.yield();
    }
    try {
      retransmissionEncoder.setSessionId(uuidAsBytes);
      retransmissionEncoder.setNextSeqNo(seqNo);
      retransmissionEncoder.setRequestTimestamp(requestTimestamp);
      retransmissionEncoder.setCount(1);
      srcs[0] = retransBuffer;
      srcs[1] = message;
      srcs[2] = null;
      transport.write(srcs);
      isHeartbeatDue.set(false);
    } finally {
      criticalSection.compareAndSet(true, false);
    }
  }

  @Override
  public void resend(ByteBuffer[] messages, int offset, int length, long seqNo,
      long requestTimestamp) throws IOException {
    Objects.requireNonNull(messages);
    while (!criticalSection.compareAndSet(false, true)) {
      Thread.yield();
    }
    try {
      retransmissionEncoder.setSessionId(uuidAsBytes);
      retransmissionEncoder.setNextSeqNo(seqNo);
      retransmissionEncoder.setRequestTimestamp(requestTimestamp);
      retransmissionEncoder.setCount(length);
      isRetransmission.set(true);
      srcs[0] = retransBuffer;
      System.arraycopy(messages, offset, srcs, 1, length);
      srcs[length + 1] = null;
      transport.write(srcs);
      isHeartbeatDue.set(false);
    } finally {
      criticalSection.compareAndSet(true, false);
    }
  }

  @Override
  public long send(ByteBuffer message) throws IOException {
    Objects.requireNonNull(message);
    one[0] = message;
    return send(one);
  }

  @Override
  public long send(ByteBuffer[] messages) throws IOException {
    Objects.requireNonNull(messages);
    while (!criticalSection.compareAndSet(false, true)) {
      Thread.yield();
    }
    try {
      transport.write(sequencer.apply(messages));
      isHeartbeatDue.set(false);
    } finally {
      criticalSection.compareAndSet(true, false);
    }
    return sequencer.getNextSeqNo();
  }

  public void sendEndOfStream() throws IOException {
    while (!criticalSection.compareAndSet(false, true)) {
      Thread.yield();
    }
    try {
      finishedBuffer = ByteBuffer.allocateDirect(34).order(ByteOrder.nativeOrder());
      FinishedSendingEncoder terminateEncoder = (FinishedSendingEncoder) messageEncoder
          .wrap(finishedBuffer, 0, MessageType.FINISHED_SENDING);
      terminateEncoder.setSessionId(uuidAsBytes);
      terminateEncoder.setLastSeqNo(sequencer.getNextSeqNo() - 1);
      transport.write(finishedBuffer);
      heartbeatBuffer = finishedBuffer;
    } finally {
      criticalSection.compareAndSet(true, false);
    }
  }

  /**
   * Heartbeats with Sequence message until finished sending, then uses FinishedSending message
   * until session is terminated.
   */
  public void sendHeartbeat() throws IOException {
    if (isHeartbeatDue.getAndSet(true)) {
      transport.write(heartbeatBuffer);
    }
  }

  /**
   * Alters sequence for test purposes only!
   */
  public void setNextSeqNo(long nextSeqNo) {
    ((MutableSequence) (this.sequencer)).setNextSeqNo(nextSeqNo);
  }
}
