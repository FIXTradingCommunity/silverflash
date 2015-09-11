package org.fixtrading.silverflash.fixp;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_STORE_RETREIVE;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.RetransmissionRequestDecoder;
import org.fixtrading.silverflash.fixp.store.MessageStore;
import org.fixtrading.silverflash.fixp.store.MessageStoreResult;
import org.fixtrading.silverflash.fixp.store.StoreException;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * Retrieves requested messages from a MessageStore and retransmits them on recoverable flows
 * 
 * @author Don Mendelson
 *
 */
public class Retransmitter implements Service {

  private class SessionValue {
    MessageStoreResult result;
    WeakReference<FixpSession> session;
  }

  private final Consumer<MessageStoreResult> consumer = new Consumer<MessageStoreResult>() {

    public void accept(MessageStoreResult result) {
      final UUID sessionId = result.getSessionId();
      final SessionValue value = resultMap.get(sessionId);
      if (value != null) {
        final FixpSession session = value.session.get();
        if (session != null) {
          try {
            resend(result, result.getFromSeqNo(), session);
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        } else {
          // session has gone away
          resultMap.remove(sessionId);
        }
      }
    }
  };

  private final MessageDecoder messageDecoder = new MessageDecoder();
  private final EventReactor<ByteBuffer> reactor;
  // todo: a vulture to remove entries for dead sessions, may consider LRU,
  // evict dead sessions
  private final Map<UUID, SessionValue> resultMap = new ConcurrentHashMap<>();

  private final Receiver retrieveHandler = buffer -> {
    Optional<Decoder> optDecoder = messageDecoder.attachForDecode(buffer, buffer.position());
    if (optDecoder.isPresent()) {
      final Decoder decoder = optDecoder.get();
      if (decoder.getMessageType() == MessageType.RETRANSMIT_REQUEST) {
        RetransmissionRequestDecoder negotiateDecoder = (RetransmissionRequestDecoder) decoder;
        byte[] sessionId = new byte[16];
        negotiateDecoder.getSessionId(sessionId, 0);
        long requestTimestamp = negotiateDecoder.getTimestamp();
        long fromSeqNo = negotiateDecoder.getFromSeqNo();
        int count = negotiateDecoder.getCount();

        UUID uuid = SessionId.UUIDFromBytes(sessionId);

        SessionValue value = getResultForSession(uuid);
        if (value.result.isRangeContained(fromSeqNo, count)) {
          try {
            resend(value.result, fromSeqNo, value.session.get());
          } catch (IOException ex) {

          }
        } else {
          try {
            if (!requestMessagesFromStore(value.result, requestTimestamp, fromSeqNo, count)) {
              // reject for in-flight request
    }
  } catch (StoreException ex) {
    // notify client?
  }
}
}
}
} ;

  private Subscription serviceStoreRetrieveSubscription;
  private final Sessions sessions;
  private final MessageStore store;

  /**
   * Constructor
   * 
   * @param reactor message pub / sub
   * @param store a repository of messages
   * @param sessions a collection of open sessions
   */
  public Retransmitter(EventReactor<ByteBuffer> reactor, MessageStore store, Sessions sessions) {
    Objects.requireNonNull(reactor);
    Objects.requireNonNull(store);
    this.reactor = reactor;
    this.store = store;
    this.sessions = sessions;
  }

  /**
   * Stop listening for requests
   */
  public void close() throws Exception {
    if (serviceStoreRetrieveSubscription != null) {
      serviceStoreRetrieveSubscription.unsubscribe();
    }
  }

  /**
   * Start listening for requests
   * 
   * @return a Future that notifies an observer when this Retransmitter is ready
   */
  public CompletableFuture<Retransmitter> open() {
    Topic retrieveTopic = SessionEventTopics.getTopic(SERVICE_STORE_RETREIVE);
    serviceStoreRetrieveSubscription = reactor.subscribe(retrieveTopic, retrieveHandler);
    return CompletableFuture.completedFuture(this);
  }

  /**
   * @return the consumer
   */
  protected Consumer<MessageStoreResult> getConsumer() {
    return consumer;
  }

  // this implementation makes retrans batch fit in one datagram
  // todo: make a pluggable batch policy
  private int batchSize(MessageStoreResult result, long fromSeqNo) {
    final long messagesRemaining = result.getMessagesRemaining(fromSeqNo);
    int count = 0;
    int totalLength = 0;

    for (; count <= messagesRemaining; count++) {
      ByteBuffer message = result.getMessage(fromSeqNo + count);
      totalLength += message.remaining();
      if (totalLength > 1400) {
        break;
      }
    }

    return count;
  }

  private SessionValue getResultForSession(UUID uuid) {
    SessionValue value = resultMap.get(uuid);
    if (value == null) {
      value = new SessionValue();
      value.result = new MessageStoreResult(uuid);
      final FixpSession session = (FixpSession) sessions.getSession(uuid);
      if (session != null) {
        value.session = new WeakReference<>(session);
        resultMap.put(uuid, value);
      }
      // todo: else race condition? session died after requesting retrans
    }

    return value;
  }

  private boolean requestMessagesFromStore(MessageStoreResult result, long requestTimestamp,
      long fromSeqNo, int count) throws StoreException {
    final boolean requested = result.setRequest(requestTimestamp, fromSeqNo, count);
    if (requested) {
      store.retrieveMessagesAsync(result, consumer);
    }
    return requested;
  }

  private void resend(MessageStoreResult result, long fromSeqNo, final FixpSession session)
      throws IOException {
    int count = batchSize(result, fromSeqNo);
    List<ByteBuffer> list = result.getMessageList(fromSeqNo, count);
    ByteBuffer[] array = new ByteBuffer[count];
    array = list.toArray(array);
    session.resend(array, 0, count, fromSeqNo, result.getRequestTimestamp());
  }
}
