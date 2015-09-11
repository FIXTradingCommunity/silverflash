package org.fixtrading.silverflash.auth.proxy;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_CREDENTIALS_RETRIEVED;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.bridge.EventReactorWithBridge;
import org.fixtrading.silverflash.transport.SharedMemoryTransport;
import org.fixtrading.silverflash.transport.Transport;

/**
 * A proxy to an authentication server
 * 
 * @author Don Mendelson
 *
 */
public class AuthenticationProxy implements Service {

  /**
   * Run AuthenticationProxy application
   * 
   * @param args not used
   * @throws Exception if the application cannot be started
   */
  public static void main(String[] args) throws Exception {
    InetAddress host = InetAddress.getLocalHost();
    int port = 8989;
    int transportNumber = 2;
    Transport transport = new SharedMemoryTransport(false, transportNumber, true);
    TrustManager[] trustManager =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            .getTrustManagers();
    try (AuthenticationProxy proxy =
        new AuthenticationProxy(host, port, transport, trustManager[0])) {
      CompletableFuture<AuthenticationProxy> future = proxy.open();
      future.get();
      System.out.println("AuthenticationProxy running");
      do {
        System.in.read();
      } while (true);
    }
  }

  private final ChallengeResponseClientProxy authServerProxy;

  private final Executor executor = Executors.newSingleThreadExecutor();
  private final EventReactorWithBridge proxyReactor;

  private final Topic responseTopic = SessionEventTopics.getTopic(SERVICE_CREDENTIALS_RETRIEVED);

  /**
   * Constructor
   * 
   * @param host host of server to connect to
   * @param port listen port of server
   * @param transport conveys messages to and from server
   * @param trustManager stores trusted keys
   */
  public AuthenticationProxy(InetAddress host, int port, Transport transport,
      TrustManager trustManager) {
    this.proxyReactor =
        EventReactorWithBridge.builder().withTransport(transport)
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    this.authServerProxy = new ChallengeResponseClientProxy(proxyReactor, host, port, trustManager);
    this.proxyReactor.forward(responseTopic);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.AutoCloseable#close()
   */
  public void close() throws Exception {
    authServerProxy.close();
    proxyReactor.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Service#open()
   */
  public CompletableFuture<AuthenticationProxy> open() {
    CompletableFuture<AuthenticationProxy> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        proxyReactor.open();
        authServerProxy.open();
        future.complete(this);
      } catch (Exception ex) {
        future.completeExceptionally(ex);
      }
    });
    return future;
  }

}
