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

package org.fixtrading.silverflash.fixp.messages;

import java.nio.ByteBuffer;

import org.fixtrading.silverflash.frame.MessageFrameEncoder;

/**
 * Factory for encoders of session messages
 * 
 * @author Don Mendelson
 *
 */
public class MessageEncoder {

  public static final class ContextEncoder extends Encoder {

    public int getBlockLength() {
      return 24;
    }

    MessageType getMessageType() {
      return MessageType.CONTEXT;
    }

    public ContextEncoder setNextSeqNo(long nextSeqNo) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 16, nextSeqNo);
      return this;
    }

    public ContextEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }
  }

  abstract static class Encoder {

    protected ByteBuffer buffer;
    protected MessageFrameEncoder frameEncoder;
    protected int offset;
    protected int variableLength = 0;

    Encoder attachForEncode(ByteBuffer buffer, int offset, MessageFrameEncoder frameEncoder,
        SbeMessageHeaderEncoder sbeMessageHeaderEncoder) {
      this.buffer = buffer;
      this.offset = offset;
      this.frameEncoder = frameEncoder;
      this.variableLength = resetVariableLength();

      final int messageLength = getMessageLength() + SbeMessageHeaderEncoder.getLength();
      frameEncoder.wrap(buffer);
      frameEncoder.setMessageLength(messageLength);
      frameEncoder.encodeFrameHeader();
      
      this.offset += frameEncoder.getHeaderLength();
      
      sbeMessageHeaderEncoder.wrap(buffer, this.offset);
      sbeMessageHeaderEncoder.setBlockLength(getBlockLength())
          .setTemplateId(getMessageType().getCode()).setSchemaId(SessionMessageSchema.SCHEMA_ID)
          .getSchemaVersion(SessionMessageSchema.SCHEMA_VERSION);

      this.offset += SbeMessageHeaderEncoder.getLength();
      
      return this;
    }

    abstract int getBlockLength();

    int getMessageLength() {
      return getBlockLength() + FIRST_FIELD_OFFSET + variableLength;
    }

    abstract MessageType getMessageType();

    protected int resetVariableLength() {
      return 0;
    }

  }

  public static final class EstablishEncoder extends Encoder {

    public int getBlockLength() {
      return 36;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.ESTABLISH;
    }

    protected int resetVariableLength() {
      return 2;
    }

    public EstablishEncoder setCredentials(byte[] credentials) {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) credentials.length);
      buffer.position(offset + FIRST_FIELD_OFFSET + getBlockLength() + 2);
      buffer.put(credentials, 0, credentials.length);
      this.variableLength = credentials.length + 2;
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public EstablishEncoder setCredentialsNull() {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) 0);
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public EstablishEncoder setKeepaliveInterval(int deltaMillisecs) {
      buffer.putInt(offset + FIRST_FIELD_OFFSET + 24, deltaMillisecs);
      return this;
    }

    public EstablishEncoder setNextSeqNo(long nextSeqNo) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 28, nextSeqNo);
      return this;
    }

    public EstablishEncoder setNextSeqNoNull() {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 28, NULL_U64);
      return this;
    }

    public EstablishEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET + 8);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }

    public EstablishEncoder setTimestamp(long nanotime) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET, nanotime);
      return this;
    }

  }

  public static final class EstablishmentAckEncoder extends Encoder {

    public int getBlockLength() {
      return 36;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.ESTABLISHMENT_ACK;
    }

    public EstablishmentAckEncoder setKeepaliveInterval(int deltaMillisecs) {
      buffer.putInt(offset + FIRST_FIELD_OFFSET + 24, deltaMillisecs);
      return this;
    }

    public EstablishmentAckEncoder setNextSeqNo(long nextSeqNo) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 28, nextSeqNo);
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public EstablishmentAckEncoder setNextSeqNoNull() {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 28, NULL_U64);
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public EstablishmentAckEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }

    public EstablishmentAckEncoder setTimestamp(long nanotime) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 16, nanotime);
      return this;
    }
  }

  public static final class EstablishmentRejectEncoder extends Encoder {

    public int getBlockLength() {
      return 25;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.ESTABLISHMENT_REJECT;
    }

    protected int resetVariableLength() {
      return 2;
    }

    public EstablishmentRejectEncoder setCode(EstablishmentReject reject) {
      buffer.put(offset + FIRST_FIELD_OFFSET + 24, reject.getCode());
      return this;
    }

    public EstablishmentRejectEncoder setReason(byte[] reason) {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) reason.length);
      buffer.put(reason, offset + FIRST_FIELD_OFFSET + getBlockLength() + 2, reason.length);
      this.variableLength = reason.length + 2;
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public EstablishmentRejectEncoder setReasonNull() {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) 0);
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public EstablishmentRejectEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }

    public EstablishmentRejectEncoder setTimestamp(long nanotime) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 16, nanotime);
      return this;
    }
  }

  public static final class FinishedReceivingEncoder extends Encoder {

    public int getBlockLength() {
      return 16;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.FINISHED_RECEIVING;
    }

    public FinishedReceivingEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }
  }

  public static final class FinishedSendingEncoder extends Encoder {

    public int getBlockLength() {
      return 24;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.FINISHED_SENDING;
    }

    public FinishedSendingEncoder setLastSeqNo(long lastSeqNo) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 16, lastSeqNo);
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public FinishedSendingEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }
  }

  public static final class NegotiateEncoder extends Encoder {

    public int getBlockLength() {
      return 25;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.NEGOTIATE;
    }

    protected int resetVariableLength() {
      return 2;
    }

    public NegotiateEncoder setClientFlow(FlowType flowType) {
      buffer.put(offset + FIRST_FIELD_OFFSET + 24, flowType.getCode());
      return this;
    }

    public NegotiateEncoder setCredentials(byte[] credentials) {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) credentials.length);
      buffer.position(offset + FIRST_FIELD_OFFSET + getBlockLength() + 2);
      buffer.put(credentials, 0, credentials.length);
      this.variableLength = credentials.length + 2;
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public NegotiateEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET + 8);
      buffer.put(uuid, 0, 16);
      return this;
    }

    public NegotiateEncoder setTimestamp(long nanotime) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET, nanotime);
      return this;
    }
  }

  public static final class NegotiationRejectEncoder extends Encoder {

    public int getBlockLength() {
      return 24;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.NEGOTIATION_REJECT;
    }

    protected int resetVariableLength() {
      return 2;
    }

    public NegotiationRejectEncoder setCode(NegotiationReject reject) {
      buffer.put(offset + FIRST_FIELD_OFFSET + 24, reject.getCode());
      return this;
    }

    public NegotiationRejectEncoder setReason(byte[] reason) {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) reason.length);
      buffer.put(reason, offset + FIRST_FIELD_OFFSET + getBlockLength() + 2, reason.length);
      this.variableLength = reason.length + 2;
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public NegotiationRejectEncoder setReasonNull() {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) 0);
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public NegotiationRejectEncoder setRequestTimestamp(long nanotime) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET, nanotime);
      return this;
    }

    public NegotiationRejectEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET + 8);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }
  }

  public static final class NegotiationResponseEncoder extends Encoder {

    public int getBlockLength() {
      return 25;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.NEGOTIATION_RESPONSE;
    }

    public NegotiationResponseEncoder setRequestTimestamp(long nanotime) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET, nanotime);
      return this;
    }

    public NegotiationResponseEncoder setServerFlow(FlowType flowType) {
      buffer.put(offset + FIRST_FIELD_OFFSET + 24, flowType.getCode());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public NegotiationResponseEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET + 8);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }
  }

  public static final class NotAppliedEncoder extends Encoder {

    public int getBlockLength() {
      return 12;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.NOT_APPLIED;
    }

    public NotAppliedEncoder setCount(int count) {
      buffer.putInt(offset + FIRST_FIELD_OFFSET + 8, count);
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public NotAppliedEncoder setFromSeqNo(long fromSeqNo) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET, fromSeqNo);
      return this;
    }
  }

  public static final class RetransmissionEncoder extends Encoder {

    public int getBlockLength() {
      return 36;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.RETRANSMISSION;
    }

    public RetransmissionEncoder setCount(int count) {
      buffer.putInt(offset + FIRST_FIELD_OFFSET + 32, count);
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public RetransmissionEncoder setNextSeqNo(long nextSeqNo) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 16, nextSeqNo);
      return this;
    }

    public RetransmissionEncoder setRequestTimestamp(long nanotime) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 24, nanotime);
      return this;
    }

    public RetransmissionEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }
  }

  public static final class RetransmissionRequestEncoder extends Encoder {

    public int getBlockLength() {
      return 36;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.RETRANSMIT_REQUEST;
    }

    public RetransmissionRequestEncoder setCount(int count) {
      buffer.putInt(offset + FIRST_FIELD_OFFSET + 32, count);
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public RetransmissionRequestEncoder setFromSeqNo(long fromSeqNo) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 24, fromSeqNo);
      return this;
    }

    public RetransmissionRequestEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }

    public RetransmissionRequestEncoder setTimestamp(long nanotime) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET + 16, nanotime);
      return this;
    }
  }

  public static final class SequenceEncoder extends Encoder {

    public int getBlockLength() {
      return Long.SIZE / 8;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.SEQUENCE;
    }

    public SequenceEncoder setNextSeqNo(long nextSeqNo) {
      buffer.putLong(offset + FIRST_FIELD_OFFSET, nextSeqNo);
      frameEncoder.encodeFrameTrailer();
      return this;
    }
  }

  public static final class TerminateEncoder extends Encoder {

    public int getBlockLength() {
      return 17;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.TERMINATE;
    }

    public TerminateEncoder setCode(TerminationCode terminationCode) {
      buffer.put(offset + FIRST_FIELD_OFFSET + 16, terminationCode.getCode());
      return this;
    }

    public TerminateEncoder setReason(byte[] reason) {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) reason.length);
      buffer.put(reason, offset + FIRST_FIELD_OFFSET + getBlockLength() + 2, reason.length);
      this.variableLength = reason.length + 2;
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public TerminateEncoder setReasonNull() {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(), (short) 0);
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public TerminateEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }
  }

  public static final class TopicEncoder extends Encoder {

    public int getBlockLength() {
      return 17;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.TOPIC;
    }

    protected int resetVariableLength() {
      return 2;
    }

    public TopicEncoder setClassification(byte[] classification) {
      buffer.putShort(offset + FIRST_FIELD_OFFSET + getBlockLength(),
          (short) classification.length);
      buffer.position(offset + FIRST_FIELD_OFFSET + getBlockLength() + 2);
      buffer.put(classification, 0, classification.length);
      this.variableLength = classification.length + 2;
      frameEncoder.setMessageLength(getMessageLength() + SbeMessageHeaderEncoder.getLength());
      frameEncoder.encodeFrameTrailer();
      return this;
    }

    public TopicEncoder setFlow(FlowType flowType) {
      buffer.put(offset + FIRST_FIELD_OFFSET + 16, flowType.getCode());
      return this;
    }

    public TopicEncoder setSessionId(byte[] uuid) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.put(uuid, 0, 16);
      buffer.position(offset + getMessageLength());
      return this;
    }
  }

  public static final class UnsequencedHeartbeatEncoder extends Encoder {

    public int getBlockLength() {
      return 0;
    }

    @Override
    MessageType getMessageType() {
      return MessageType.UNSEQUENCED_HEARTBEAT;
    }
  }

  private static final int FIRST_FIELD_OFFSET = 0;

  private static final long NULL_U64 = 0xffffffffffffffffL;

  private final ThreadLocal<ContextEncoder> contextEncoder = new ThreadLocal<ContextEncoder>() {
    @Override
    protected ContextEncoder initialValue() {
      return new ContextEncoder();
    }
  };

  private final ThreadLocal<EstablishEncoder> establishEncoder = new ThreadLocal<EstablishEncoder>() {
    @Override
    protected EstablishEncoder initialValue() {
      return new EstablishEncoder();
    }
  };

  private final ThreadLocal<EstablishmentAckEncoder> establishmentAckEncoder = new ThreadLocal<EstablishmentAckEncoder>() {
    @Override
    protected EstablishmentAckEncoder initialValue() {
      return new EstablishmentAckEncoder();
    }
  };

  private final ThreadLocal<EstablishmentRejectEncoder> establishmentRejectEncoder = new ThreadLocal<EstablishmentRejectEncoder>() {
    @Override
    protected EstablishmentRejectEncoder initialValue() {
      return new EstablishmentRejectEncoder();
    }
  };

  private final ThreadLocal<FinishedReceivingEncoder> finishedReceivingEncoder = new ThreadLocal<FinishedReceivingEncoder>() {
    @Override
    protected FinishedReceivingEncoder initialValue() {
      return new FinishedReceivingEncoder();
    }
  };

  private final ThreadLocal<FinishedSendingEncoder> finishedSendingEncoder = new ThreadLocal<FinishedSendingEncoder>() {
    @Override
    protected FinishedSendingEncoder initialValue() {
      return new FinishedSendingEncoder();
    }
  };

  private final ThreadLocal<MessageFrameEncoder> messageFrameEncoder = new ThreadLocal<MessageFrameEncoder>() {
    @Override
    protected MessageFrameEncoder initialValue() {
      try {
        return messageFrameEncoderClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  };

  private Class<? extends MessageFrameEncoder> messageFrameEncoderClass;

  private final ThreadLocal<NegotiateEncoder> negotiateEncoder = new ThreadLocal<NegotiateEncoder>() {
    @Override
    protected NegotiateEncoder initialValue() {
      return new NegotiateEncoder();
    }
  };

  private final ThreadLocal<NegotiationRejectEncoder> negotiationRejectEncoder = new ThreadLocal<NegotiationRejectEncoder>() {
    @Override
    protected NegotiationRejectEncoder initialValue() {
      return new NegotiationRejectEncoder();
    }
  };

  private final ThreadLocal<NegotiationResponseEncoder> negotiationResponseEncoder = new ThreadLocal<NegotiationResponseEncoder>() {
    @Override
    protected NegotiationResponseEncoder initialValue() {
      return new NegotiationResponseEncoder();
    }
  };

  private final ThreadLocal<NotAppliedEncoder> notAppliedEncoder = new ThreadLocal<NotAppliedEncoder>() {
    @Override
    protected NotAppliedEncoder initialValue() {
      return new NotAppliedEncoder();
    }
  };

  private final ThreadLocal<RetransmissionEncoder> retransmissionEncoder = new ThreadLocal<RetransmissionEncoder>() {
    @Override
    protected RetransmissionEncoder initialValue() {
      return new RetransmissionEncoder();
    }
  };

  private final ThreadLocal<RetransmissionRequestEncoder> retransmissionRequestEncoder = new ThreadLocal<RetransmissionRequestEncoder>() {
    @Override
    protected RetransmissionRequestEncoder initialValue() {
      return new RetransmissionRequestEncoder();
    }
  };

  private final ThreadLocal<SbeMessageHeaderEncoder> sbeMessageHeaderEncoder = new ThreadLocal<SbeMessageHeaderEncoder>() {
    @Override
    protected SbeMessageHeaderEncoder initialValue() {
      return new SbeMessageHeaderEncoder();
    }
  };

  private final ThreadLocal<SequenceEncoder> sequenceEncoder = new ThreadLocal<SequenceEncoder>() {
    @Override
    protected SequenceEncoder initialValue() {
      return new SequenceEncoder();
    }
  };

  private final ThreadLocal<TerminateEncoder> terminateEncoder = new ThreadLocal<TerminateEncoder>() {
    @Override
    protected TerminateEncoder initialValue() {
      return new TerminateEncoder();
    }
  };

  private final ThreadLocal<TopicEncoder> topicEncoder = new ThreadLocal<TopicEncoder>() {
    @Override
    protected TopicEncoder initialValue() {
      return new TopicEncoder();
    }
  };

  private final ThreadLocal<UnsequencedHeartbeatEncoder> unsequencedHeartbeatEncoder = new ThreadLocal<UnsequencedHeartbeatEncoder>() {
    @Override
    protected UnsequencedHeartbeatEncoder initialValue() {
      return new UnsequencedHeartbeatEncoder();
    }
  };

  /**
   * @param messageFrameEncoderClass
   */
  public MessageEncoder(Class<? extends MessageFrameEncoder> messageFrameEncoderClass) {
    this.messageFrameEncoderClass = messageFrameEncoderClass;
  }

  /**
   * Creates a new messageEncoder for the specified message type and attaches it to a buffer
   * 
   * @param buffer
   *          buffer to hold the message
   * @param offset
   *          index into the buffer to encode
   * @param messageType
   *          type of message
   * @return an initialized messageEncoder
   */
  public Encoder wrap(ByteBuffer buffer, int offset, MessageType messageType) {

    Encoder encoder;
    switch (messageType) {
    case SEQUENCE:
      encoder = sequenceEncoder.get();
      break;
    case RETRANSMISSION:
      encoder = retransmissionEncoder.get();
      break;
    case RETRANSMIT_REQUEST:
      encoder = retransmissionRequestEncoder.get();
      break;
    case NOT_APPLIED:
      encoder = notAppliedEncoder.get();
      break;
    case NEGOTIATE:
      encoder = negotiateEncoder.get();
      break;
    case NEGOTIATION_RESPONSE:
      encoder = negotiationResponseEncoder.get();
      break;
    case NEGOTIATION_REJECT:
      encoder = negotiationRejectEncoder.get();
      break;
    case ESTABLISH:
      encoder = establishEncoder.get();
      break;
    case ESTABLISHMENT_ACK:
      encoder = establishmentAckEncoder.get();
      break;
    case ESTABLISHMENT_REJECT:
      encoder = establishmentRejectEncoder.get();
      break;
    case UNSEQUENCED_HEARTBEAT:
      encoder = unsequencedHeartbeatEncoder.get();
      break;
    case TERMINATE:
      encoder = terminateEncoder.get();
      break;
    case FINISHED_SENDING:
      encoder = finishedSendingEncoder.get();
      break;
    case FINISHED_RECEIVING:
      encoder = finishedReceivingEncoder.get();
      break;
    case CONTEXT:
      encoder = contextEncoder.get();
      break;
    case TOPIC:
      encoder = topicEncoder.get();
      break;
    default:
      throw new RuntimeException("Internal error");
    }

    encoder.attachForEncode(buffer, offset, messageFrameEncoder.get(),
        sbeMessageHeaderEncoder.get());
    return encoder;
  }

}
