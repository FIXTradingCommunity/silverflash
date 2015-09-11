package org.fixtrading.silverflash.auth;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_CREDENTIALS_RETRIEVED;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.auth.SimpleDirectory;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.bridge.EventReactorWithBridge;
import org.fixtrading.silverflash.transport.SharedMemoryTransport;
import org.fixtrading.silverflash.transport.Transport;

/**
 * An application to authenticate users out-of-band
 * 
 * @author Don Mendelson
 *
 */
public class AuthenticationServer implements Service {
  private static final String STORE_PASSPHRASE_PROPERTY = "STORE_PASSPHRASE";

  /**
   * Starts an authentication server
   * 
   * @param args not used
   * @throws Exception if the application cannot be started
   */
  public static void main(String[] args) throws Exception {
    InetAddress host = InetAddress.getLocalHost();
    int port = 8989;
    char[] storePassphrase =
        System.getProperties().getProperty(STORE_PASSPHRASE_PROPERTY).toCharArray();
    int transportNumber = 1;
    final String distinguishedNames = "CN=trading, O=myorg, C=US";
    Transport transport = new SharedMemoryTransport(false, transportNumber, true);
    try (AuthenticationServer server =
        new AuthenticationServer(host, port, storePassphrase, distinguishedNames, transport)) {
      CompletableFuture<AuthenticationServer> future = server.open();
      future.get();
      System.out.println("AuthenticationServer running");
      do {
        System.in.read();
      } while (true);
    }
  }

  private final ChallengeResponseAuthenticatorObservable authenticator;
  private final EventReactorWithBridge authServerReactor;
  private final Executor executor = Executors.newSingleThreadExecutor();

  private final Topic responseTopic = SessionEventTopics.getTopic(SERVICE_CREDENTIALS_RETRIEVED);


  /**
   * Constructor
   * 
   * @param host address to listen for connections
   * @param port listen port
   * @param storePassphrase passphrase for key store
   * @param distinguishedNames a string representation of an entry in a directory
   * @param transport conveys messages to and from clients
   */
  public AuthenticationServer(InetAddress host, int port, char[] storePassphrase,
      String distinguishedNames, Transport transport) {
    authServerReactor =
        EventReactorWithBridge.builder().withTransport(transport)
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    authServerReactor.forward(responseTopic);
    authenticator =
        new ChallengeResponseAuthenticatorObservable(host, port, storePassphrase,
            distinguishedNames, new SimpleDirectory(), authServerReactor);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.AutoCloseable#close()
   */
  public void close() throws Exception {
    authenticator.close();
    authServerReactor.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Service#open()
   */
  public CompletableFuture<AuthenticationServer> open() {
    CompletableFuture<AuthenticationServer> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        authServerReactor.open();
        // authServerReactor.setTrace(true, "authServerReactor");
        authenticator.open();
        future.complete(this);
      } catch (Exception ex) {
        future.completeExceptionally(ex);
      }
    });
    return future;
  }

}
