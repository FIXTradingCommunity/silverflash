package org.fixtrading.silverflash.auth.proxy;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_CREDENTIALS_REQUEST;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_CREDENTIALS_RETRIEVED;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.auth.ChallengeResponseClient;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * A proxy to an authentication server
 * 
 * @author Don Mendelson
 *
 */
public class ChallengeResponseClientProxy implements Service {

  private final ByteBuffer outBuffer = ByteBuffer.allocateDirect(2048).order(
      ByteOrder.nativeOrder());
  private final InetAddress address;
  private final int port;
  private final EventReactor<ByteBuffer> reactor;

  private final Receiver requestHandler = new Receiver() {

    private final CredentialsRequestMessage requestMessage = new CredentialsRequestMessage();

    public void accept(ByteBuffer buffer) {
      requestMessage.attachForDecode(buffer, 0);

      String name;
      try {
        name = requestMessage.getName();

        char[] password = requestMessage.getPassword();
        ChallengeResponseClient challengeResponseClient =
            new ChallengeResponseClient(address, port, trustManager);
        if (challengeResponseClient.authenticate(name, password)) {
          byte[] token = challengeResponseClient.getToken();
          onAuthenticated(name, token);
        }
      } catch (GeneralSecurityException | IOException | InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

  };

  private final CredentialsResponseMessage responseMessage = new CredentialsResponseMessage();
  private final Topic responseTopic;
  private Subscription subscription;
  private final TrustManager trustManager;

  /**
   * Constructor
   * 
   * @param reactor an EventReactor
   * @param address remote address
   * @param port remote port
   * @throws NoSuchAlgorithmException if the default algorithm is not available
   */
  public ChallengeResponseClientProxy(EventReactor<ByteBuffer> reactor, InetAddress address,
      int port) throws NoSuchAlgorithmException {
    this(reactor, address, port, TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm()).getTrustManagers()[0]);
  }

  /**
   * Constructor
   * 
   * @param reactor an EventReactor
   * @param address remote address
   * @param port remote port
   * @param trustManager holds trusted keys
   */
  public ChallengeResponseClientProxy(EventReactor<ByteBuffer> reactor, InetAddress address,
      int port, TrustManager trustManager) {
    this.reactor = reactor;
    this.address = address;
    this.port = port;
    this.trustManager = trustManager;
    this.responseTopic = SessionEventTopics.getTopic(SERVICE_CREDENTIALS_RETRIEVED);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.AutoCloseable#close()
   */
  public void close() throws Exception {
    subscription.unsubscribe();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Service#open()
   */
  public CompletableFuture<ChallengeResponseClientProxy> open() {
    Topic topic = SessionEventTopics.getTopic(SERVICE_CREDENTIALS_REQUEST);
    subscription = reactor.subscribe(topic, requestHandler);
    return CompletableFuture.completedFuture(this);
  }

  private void onAuthenticated(String name, byte[] token) throws UnsupportedEncodingException {
    outBuffer.clear();
    responseMessage.attachForEncode(outBuffer, 0);
    responseMessage.setName(name);
    responseMessage.setTicket(token);
    reactor.post(responseTopic, outBuffer);
  }
}
