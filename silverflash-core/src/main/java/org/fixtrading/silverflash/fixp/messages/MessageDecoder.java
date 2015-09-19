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
import java.util.Optional;

/**
 * Factory for FIXP session message decoders
 * 
 * @author Don Mendelson
 *
 */
public class MessageDecoder {

  public abstract class Decoder {
    protected ByteBuffer buffer;
    protected int offset;

    public ByteBuffer getBuffer() {
      return buffer;
    }

    public abstract MessageType getMessageType();

    public int getOffset() {
      return offset;
    }

    Decoder attachForDecode(ByteBuffer buffer, int offset) {
      this.buffer = buffer;
      this.offset = offset;

      return this;
    }
  }

  public final class ContextDecoder extends Decoder {

    public int getBlockLength() {
      return 24;
    }

    public MessageType getMessageType() {
      return MessageType.CONTEXT;
    }

    public ContextDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.get(dest, destOffset, 16);
      return this;
    }

    public long getNextSeqNo() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 16);
    }
  }

  public final class EstablishDecoder extends Decoder {

    public int getBlockLength() {
      return 36;
    }

    public EstablishDecoder getCredentials(byte[] dest, int destOffset) {
      short variableLength = buffer.getShort(offset + FIRST_FIELD_OFFSET + getBlockLength());
      buffer.position(offset + FIRST_FIELD_OFFSET + getBlockLength() + 2);
      buffer.get(dest, destOffset, variableLength);
      return this;
    }

    public int getKeepaliveInterval() {
      return buffer.getInt(offset + FIRST_FIELD_OFFSET + 24);
    }

    public MessageType getMessageType() {
      return MessageType.ESTABLISH;
    }

    public long getNextSeqNo() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 28);
    }

    public long getRequestTimestamp() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET);
    }

    public EstablishDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET + 8);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class EstablishmentAckDecoder extends Decoder {

    public int getBlockLength() {
      return 36;
    }

    public int getKeepaliveInterval() {
      return buffer.getInt(offset + FIRST_FIELD_OFFSET + 24);
    }

    public MessageType getMessageType() {
      return MessageType.ESTABLISHMENT_ACK;
    }

    public long getNextSeqNo() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 28);
    }

    public long getRequestTimestamp() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 16);
    }

    public EstablishmentAckDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class EstablishmentRejectDecoder extends Decoder {

    public int getBlockLength() {
      return 25;
    }

    public EstablishmentReject getCode() {
      return EstablishmentReject.getReject(buffer.get(offset + FIRST_FIELD_OFFSET + 24));
    }

    public MessageType getMessageType() {
      return MessageType.ESTABLISHMENT_REJECT;
    }

    public EstablishmentRejectDecoder getReason(byte[] dest, int destOffset) {
      short variableLength = buffer.getShort(offset + FIRST_FIELD_OFFSET + getBlockLength());
      buffer.get(dest, destOffset, variableLength);
      return this;
    }

    public long getRequestTimestamp() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 16);
    }

    public EstablishmentRejectDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class FinishedReceivingDecoder extends Decoder {

    public int getBlockLength() {
      return 16;
    }

    @Override
    public MessageType getMessageType() {
      return MessageType.FINISHED_RECEIVING;
    }

    public FinishedReceivingDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class FinishedSendingDecoder extends Decoder {

    public int getBlockLength() {
      return 17;
    }

    public long getLastSeqNo() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 16);
    }

    @Override
    public MessageType getMessageType() {
      return MessageType.FINISHED_SENDING;
    }

    public FinishedSendingDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class NegotiateDecoder extends Decoder {

    public int getBlockLength() {
      return 25;
    }

    public FlowType getClientFlow() {
      return FlowType.getFlowType(buffer.get(offset + FIRST_FIELD_OFFSET + 24));
    }

    public NegotiateDecoder getCredentials(byte[] dest, int destOffset) {
      short variableLength = buffer.getShort(offset + FIRST_FIELD_OFFSET + getBlockLength());
      buffer.position(offset + FIRST_FIELD_OFFSET + getBlockLength() + 2);
      buffer.get(dest, destOffset, variableLength);
      return this;
    }

    public MessageType getMessageType() {
      return MessageType.NEGOTIATE;
    }

    public NegotiateDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET + 8);
      buffer.get(dest, destOffset, 16);
      return this;
    }

    public long getTimestamp() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET);
    }
  }

  public final class NegotiationRejectDecoder extends Decoder {

    public int getBlockLength() {
      return 24;
    }

    public NegotiationReject getCode() {
      return NegotiationReject.getReject(buffer.get(offset + FIRST_FIELD_OFFSET + 24));
    }

    public MessageType getMessageType() {
      return MessageType.NEGOTIATION_REJECT;
    }

    public NegotiationRejectDecoder getReason(byte[] dest, int destOffset) {
      short variableLength = buffer.getShort(offset + FIRST_FIELD_OFFSET + getBlockLength());
      buffer.get(dest, destOffset, variableLength);
      return this;
    }

    public long getRequestTimestamp() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET);
    }

    public NegotiationRejectDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET + 8);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class NegotiationResponseDecoder extends Decoder {

    public int getBlockLength() {
      return 25;
    }

    public MessageType getMessageType() {
      return MessageType.NEGOTIATION_RESPONSE;
    }

    public long getRequestTimestamp() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET);
    }

    public FlowType getServerFlow() {
      return FlowType.getFlowType(buffer.get(offset + FIRST_FIELD_OFFSET + 24));
    }

    public NegotiationResponseDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET + 8);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class NotAppliedDecoder extends Decoder {

    public int getCount() {
      return buffer.getInt(offset + FIRST_FIELD_OFFSET + 8);
    }

    public long getFromSeqNo() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET);
    }

    public MessageType getMessageType() {
      return MessageType.NOT_APPLIED;
    }
  }

  public final class RetransmissionDecoder extends Decoder {

    public int getCount() {
      return buffer.getInt(offset + FIRST_FIELD_OFFSET + 32);
    }

    public MessageType getMessageType() {
      return MessageType.RETRANSMISSION;
    }

    public long getNextSeqNo() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 16);
    }

    public long getRequestTimestamp() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 24);
    }

    public RetransmissionDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class RetransmissionRequestDecoder extends Decoder {

    public int getCount() {
      return buffer.getInt(offset + FIRST_FIELD_OFFSET + 32);
    }

    public long getFromSeqNo() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 24);
    }

    public MessageType getMessageType() {
      return MessageType.RETRANSMIT_REQUEST;
    }

    public RetransmissionRequestDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.get(dest, destOffset, 16);
      return this;
    }

    public long getTimestamp() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET + 16);
    }
  }

  public final class SequenceDecoder extends Decoder {
    public MessageType getMessageType() {
      return MessageType.SEQUENCE;
    }

    public long getNextSeqNo() {
      return buffer.getLong(offset + FIRST_FIELD_OFFSET);
    }

    @Override
    public String toString() {
      return "SequenceDecoder [getNextSeqNo()=" + getNextSeqNo() + ", MessageHeaderWithFrame="
          + messageHeader.toString() + "]";
    }
  }

  public final class TerminateDecoder extends Decoder {

    public int getBlockLength() {
      return 17;
    }

    public TerminationCode getCode() {
      return TerminationCode.getTerminateCode(buffer.get(offset + FIRST_FIELD_OFFSET + 16));
    }

    public MessageType getMessageType() {
      return MessageType.TERMINATE;
    }

    public TerminateDecoder getReason(byte[] dest, int destOffset) {
      short variableLength = buffer.getShort(offset + FIRST_FIELD_OFFSET + getBlockLength());
      buffer.get(dest, destOffset, variableLength);
      return this;
    }

    public TerminateDecoder getSessionId(byte[] dest, int destOffset) {
      buffer.position(offset + FIRST_FIELD_OFFSET);
      buffer.get(dest, destOffset, 16);
      return this;
    }
  }

  public final class UnsequencedHeartbeatDecoder extends Decoder {
    public MessageType getMessageType() {
      return MessageType.UNSEQUENCED_HEARTBEAT;
    }

    @Override
    public String toString() {
      return "UnsequencedHeartbeatDecoder [MessageHeaderWithFrame=" + messageHeader.toString()
          + "]";
    }
  }

  private static final int FIRST_FIELD_OFFSET = MessageHeaderWithFrame.getLength();

  private final ThreadLocal<Optional<Decoder>> establish = new ThreadLocal<Optional<Decoder>>() {
    @Override
    protected Optional<Decoder> initialValue() {
      return Optional.of(new EstablishDecoder());
    }
  };

  private final ThreadLocal<Optional<Decoder>> establishmentAck =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new EstablishmentAckDecoder());
        }
      };

  private final ThreadLocal<Optional<Decoder>> establishmentReject =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new EstablishmentRejectDecoder());
        }
      };

  private final ThreadLocal<Optional<Decoder>> finishedReceiving =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new FinishedReceivingDecoder());
        }
      };

  private final ThreadLocal<Optional<Decoder>> context = new ThreadLocal<Optional<Decoder>>() {
    @Override
    protected Optional<Decoder> initialValue() {
      return Optional.of(new ContextDecoder());
    }
  };

  private final ThreadLocal<Optional<Decoder>> finishedSending =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new FinishedSendingDecoder());
        }
      };

  private final ThreadLocal<MessageHeaderWithFrame> messageHeader =
      new ThreadLocal<MessageHeaderWithFrame>() {
        @Override
        protected MessageHeaderWithFrame initialValue() {
          return new MessageHeaderWithFrame();
        }
      };

  private final ThreadLocal<Optional<Decoder>> negotiate = new ThreadLocal<Optional<Decoder>>() {
    @Override
    protected Optional<Decoder> initialValue() {
      return Optional.of(new NegotiateDecoder());
    }
  };

  private final ThreadLocal<Optional<Decoder>> negotiationReject =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new NegotiationRejectDecoder());
        }
      };

  private final ThreadLocal<Optional<Decoder>> negotiationResponse =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new NegotiationResponseDecoder());
        }
      };

  private final ThreadLocal<Optional<Decoder>> notApplied = new ThreadLocal<Optional<Decoder>>() {
    @Override
    protected Optional<Decoder> initialValue() {
      return Optional.of(new NotAppliedDecoder());
    }
  };

  private final ThreadLocal<Optional<Decoder>> retransmission =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new RetransmissionDecoder());
        }
      };

  private final ThreadLocal<Optional<Decoder>> retransmissionRequest =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new RetransmissionRequestDecoder());
        }
      };

  private final ThreadLocal<Optional<Decoder>> sequence = new ThreadLocal<Optional<Decoder>>() {
    @Override
    protected Optional<Decoder> initialValue() {
      return Optional.of(new SequenceDecoder());
    }
  };

  private final ThreadLocal<Optional<Decoder>> terminate = new ThreadLocal<Optional<Decoder>>() {
    @Override
    protected Optional<Decoder> initialValue() {
      return Optional.of(new TerminateDecoder());
    }
  };

  private final ThreadLocal<Optional<Decoder>> unsequencedHeartbeat =
      new ThreadLocal<Optional<Decoder>>() {
        @Override
        protected Optional<Decoder> initialValue() {
          return Optional.of(new UnsequencedHeartbeatDecoder());
        }
      };

  public Optional<Decoder> attachForDecode(ByteBuffer buffer, int offset) {
    MessageHeaderWithFrame header = messageHeader.get();
    int schema = header.attachForDecode(buffer, offset).getSchemaId();
    if (schema != SessionMessageSchema.SCHEMA_ID) {
      return Optional.empty();
    }
    int code = header.getTemplateId();

    MessageType messageType = MessageType.getMsgType(code);

    Optional<Decoder> opt;
    switch (messageType) {
      case SEQUENCE:
        opt = sequence.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case RETRANSMISSION:
        opt = retransmission.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case RETRANSMIT_REQUEST:
        opt = retransmissionRequest.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case NOT_APPLIED:
        opt = notApplied.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case NEGOTIATE:
        opt = negotiate.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case NEGOTIATION_RESPONSE:
        opt = negotiationResponse.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case NEGOTIATION_REJECT:
        opt = negotiationReject.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case ESTABLISH:
        opt = establish.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case ESTABLISHMENT_ACK:
        opt = establishmentAck.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case ESTABLISHMENT_REJECT:
        opt = establishmentReject.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case UNSEQUENCED_HEARTBEAT:
        opt = unsequencedHeartbeat.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case TERMINATE:
        opt = terminate.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case FINISHED_SENDING:
        opt = finishedSending.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case FINISHED_RECEIVING:
        opt = finishedReceiving.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      case CONTEXT:
        opt = context.get();
        opt.get().attachForDecode(buffer, offset);
        break;
      default:
        opt = Optional.empty();
    }
    return opt;
  }

}
