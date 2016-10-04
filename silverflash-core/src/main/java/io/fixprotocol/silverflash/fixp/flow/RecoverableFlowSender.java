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
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.SessionEventType.HEARTBEAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.Receiver;
import io.fixprotocol.silverflash.RecoverableSender;
import io.fixprotocol.silverflash.fixp.SessionEventTopics;
import io.fixprotocol.silverflash.fixp.messages.FinishedSendingEncoder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.fixp.messages.RetransmissionEncoder;
import io.fixprotocol.silverflash.fixp.store.MessageStore;
import io.fixprotocol.silverflash.fixp.store.StoreException;
import io.fixprotocol.silverflash.reactor.Subscription;
import io.fixprotocol.silverflash.reactor.TimerSchedule;
import io.fixprotocol.silverflash.reactor.Topic;

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
  }

  private static final ByteBuffer[] EMPTY = new ByteBuffer[0];

  @SuppressWarnings("rawtypes")
  public static Builder builder() {
    return new Builder();
  }

  private final AtomicBoolean criticalSection = new AtomicBoolean();
  private final FinishedSendingEncoder finishedSendingEncoder = new FinishedSendingEncoder();
  private final Receiver heartbeatEvent = t -> {
    try {
      sendHeartbeat();
    } catch (IOException e) {
      Topic terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);
      reactor.post(terminatedTopic, t);
    }
  };
  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final AtomicBoolean isRetransmission = new AtomicBoolean();
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final ByteBuffer[] one = new ByteBuffer[1];
  private final RetransmissionEncoder retransmissionEncoder = new RetransmissionEncoder();
  private final ByteBuffer sendBuffer =
      ByteBuffer.allocateDirect(64).order(ByteOrder.nativeOrder());
  private final ByteBuffer[] srcs = new ByteBuffer[32];

  private final MessageStore store;
  private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);

  protected RecoverableFlowSender(Builder builder) {
    super(builder);
    Objects.requireNonNull(builder.store);
    this.store = builder.store;
    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule =
        reactor.postAtInterval(heartbeatTopic, ByteBuffer.allocate(0), keepaliveInterval);
  }

  public long getNextSeqNo() {
    return sequencer.getNextSeqNo();
  }

  protected boolean isHeartbeatDue() {
    return isHeartbeatDue.getAndSet(true);
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
      int offset = 0;
      frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
      offset += frameEncoder.getHeaderLength();
      messageHeaderEncoder.wrap(mutableBuffer, offset);
      messageHeaderEncoder.blockLength(retransmissionEncoder.sbeBlockLength())
          .templateId(retransmissionEncoder.sbeTemplateId())
          .schemaId(retransmissionEncoder.sbeSchemaId())
          .version(retransmissionEncoder.sbeSchemaVersion());
      offset += messageHeaderEncoder.encodedLength();
      retransmissionEncoder.wrap(mutableBuffer, offset);

      for (int i = 0; i < 16; i++) {
        retransmissionEncoder.sessionId(i, uuidAsBytes[i]);
      }
      retransmissionEncoder.nextSeqNo(seqNo);
      retransmissionEncoder.requestTimestamp(requestTimestamp);
      retransmissionEncoder.count(1L);
      frameEncoder.setMessageLength(offset + retransmissionEncoder.encodedLength());
      frameEncoder.encodeFrameTrailer();
      
      srcs[0] = sendBuffer;
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
      frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
      offset += frameEncoder.getHeaderLength();
      messageHeaderEncoder.wrap(mutableBuffer, offset);
      messageHeaderEncoder.blockLength(retransmissionEncoder.sbeBlockLength())
          .templateId(retransmissionEncoder.sbeTemplateId())
          .schemaId(retransmissionEncoder.sbeSchemaId())
          .version(retransmissionEncoder.sbeSchemaVersion());
      offset += messageHeaderEncoder.encodedLength();
      retransmissionEncoder.wrap(mutableBuffer, offset);

      for (int i = 0; i < 16; i++) {
        retransmissionEncoder.sessionId(i, uuidAsBytes[i]);
      }
      retransmissionEncoder.nextSeqNo(seqNo);
      retransmissionEncoder.requestTimestamp(requestTimestamp);
      retransmissionEncoder.count(length);
      frameEncoder.setMessageLength(offset + retransmissionEncoder.encodedLength());
      frameEncoder.encodeFrameTrailer();
      
      isRetransmission.set(true);
      srcs[0] = sendBuffer;
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
      int offset = 0;
      frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
      offset += frameEncoder.getHeaderLength();
      messageHeaderEncoder.wrap(mutableBuffer, offset);
      messageHeaderEncoder.blockLength(finishedSendingEncoder.sbeBlockLength())
          .templateId(finishedSendingEncoder.sbeTemplateId())
          .schemaId(finishedSendingEncoder.sbeSchemaId())
          .version(finishedSendingEncoder.sbeSchemaVersion());
      offset += messageHeaderEncoder.encodedLength();
      finishedSendingEncoder.wrap(mutableBuffer, offset);
      for (int i = 0; i < 16; i++) {
        finishedSendingEncoder.sessionId(i, uuidAsBytes[i]);
      }
      finishedSendingEncoder.lastSeqNo(sequencer.getNextSeqNo() - 1);
      frameEncoder.setMessageLength(offset + finishedSendingEncoder.encodedLength());
      frameEncoder.encodeFrameTrailer();
      
      transport.write(sendBuffer);
    } finally {
      criticalSection.compareAndSet(true, false);
    }
  }

  /**
   * Heartbeats with Sequence message until finished sending, then uses FinishedSending message
   * until session is terminated.
   * 
   * @throws IOException if a message cannot be sent
   */
  public void sendHeartbeat() throws IOException {
    if (isHeartbeatDue()) {
      send(EMPTY);
    }
  }

  /**
   * Alters sequence for test purposes only!
   */
  public void setNextSeqNo(long nextSeqNo) {
    ((MutableSequence) (this.sequencer)).setNextSeqNo(nextSeqNo);
  }
}
