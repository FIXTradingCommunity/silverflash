package org.fixtrading.silverflash.fixp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.frame.FixpWithMessageLengthFrameSpliterator;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageSessionIdentifier;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.transport.IdentifiableTransportConsumer;
import org.fixtrading.silverflash.transport.SharedTransportDecorator;
import org.fixtrading.silverflash.transport.TransportConsumer;
import org.fixtrading.silverflash.transport.SharedTransportDecorator.Builder;

/**
 * Wraps a shared Transport for multiple sessions
 * <p>
 * Received application messages are routed to sessions by session ID (UUID) carried by FIXP session
 * messages.
 * <p>
 * This class may be used with sessions in either client or server mode. A new FixpSession in server
 * mode is created on the arrival of a Negotiate message on the Transport. New sessions are
 * configured to multiplex (sends a Context message when context switching).
 * 
 * 
 * @author Don Mendelson
 *
 */
public class FixpSharedTransportAdaptor extends SharedTransportDecorator<UUID> {

  public static class Builder extends
      SharedTransportDecorator.Builder<UUID, FixpSharedTransportAdaptor, Builder> {

    public Supplier<MessageConsumer<UUID>> consumerSupplier;
    public FlowType flowType;
    public EventReactor<ByteBuffer> reactor;

    protected Builder() {
      super();
      this.withMessageFramer(new FixpWithMessageLengthFrameSpliterator());
      this.withMessageIdentifer(new MessageSessionIdentifier());
    }

    /**
     * Build a new FixpSharedTransportAdaptor object
     * 
     * @return a new adaptor
     */
    public FixpSharedTransportAdaptor build() {
      return new FixpSharedTransportAdaptor(this);
    }

    public Builder withMessageConsumerSupplier(Supplier<MessageConsumer<UUID>> consumerSupplier) {
      this.consumerSupplier = consumerSupplier;
      return this;
    }

    public Builder withFlowType(FlowType flowType) {
      this.flowType = flowType;
      return this;
    }

    public Builder withReactor(EventReactor<ByteBuffer> reactor) {
      this.reactor = reactor;
      return this;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private final Supplier<MessageConsumer<UUID>> consumerSupplier;
  private final FlowType flowType;

  private final Consumer<UUID> newSessionConsumer = new Consumer<UUID>() {

    @SuppressWarnings("unchecked")
    public void accept(UUID sessionId) {
      FixpSession session =
          FixpSession
              .builder()
              .withReactor(reactor)
              .withTransport(FixpSharedTransportAdaptor.this, true)
              .withBufferSupplier(
                  new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                      ByteOrder.nativeOrder()))).withMessageConsumer(consumerSupplier.get())
              .withOutboundFlow(flowType).withSessionId(sessionId).asServer().build();

      try {
        session.open();
      } catch (IOException e) {
        exceptionConsumer.accept(e);
      }

    }

  };

  private final EventReactor<ByteBuffer> reactor;

  private final Consumer<? super ByteBuffer> router = new Consumer<ByteBuffer>() {

    private UUID lastId;

    /**
     * Gets session ID from message, looks up session and invokes session consumer. If a message
     * doesn't contain a session ID, then it continues to send to the last identified session until
     * the context changes.
     */
    public void accept(ByteBuffer buffer) {
      UUID id = getMessageIdentifier().apply(buffer);
      if (id != null) {
        lastId = id;
      }
      if (lastId != null) {
        ConsumerWrapper wrapper = getConsumerWrapper(lastId);

        if (wrapper != null) {
          wrapper.getConsumer().accept(buffer);
        } else {
          wrapper = uninitialized.poll();
          if (wrapper != null) {
            final TransportConsumer consumer = wrapper.getConsumer();
            try {
              open(wrapper.getBuffers(), consumer, id);
              consumer.accept(buffer);
            } catch (IOException e) {
              exceptionConsumer.accept(e);
            }
          } else {
            System.out.println("Unknown sesion ID and no uninitialized session available");
          }
        }
      }
    }

  };

  private final Queue<ConsumerWrapper> uninitialized = new ConcurrentLinkedQueue<>();

  protected FixpSharedTransportAdaptor(Builder builder) {
    super(builder);
    this.reactor = builder.reactor;
    this.flowType = builder.flowType;
    this.consumerSupplier = builder.consumerSupplier;
    if (this.getMessageIdentifier() instanceof MessageSessionIdentifier) {
      this.messageIdentifier =
          new MessageSessionIdentifier().withNewSessionConsumer(newSessionConsumer);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.SharedTransportDecorator#open(java.util.function
   * .Supplier, org.fixtrading.silverflash.transport.IdentifiableTransportConsumer)
   */
  @Override
  public void open(Supplier<ByteBuffer> buffers, IdentifiableTransportConsumer<UUID> consumer)
      throws IOException {
    if (consumer.getSessionId().equals(SessionId.EMPTY)) {
      uninitialized.add(new ConsumerWrapper(buffers, consumer));
      openUnderlyingTransport();
      consumer.connected();
    } else {
      super.open(buffers, consumer);
    }
  }

  protected Consumer<? super ByteBuffer> getRouter() {
    return router;
  }

}
