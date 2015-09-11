package org.fixtrading.silverflash.fixp.auth;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_CREDENTIALS_REQUEST;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_CREDENTIALS_RETRIEVED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.auth.AuthenticationServer;
import org.fixtrading.silverflash.auth.Credentials;
import org.fixtrading.silverflash.auth.Directory;
import org.fixtrading.silverflash.auth.Entity;
import org.fixtrading.silverflash.auth.SimpleDirectory;
import org.fixtrading.silverflash.auth.proxy.AuthenticationProxy;
import org.fixtrading.silverflash.auth.proxy.CredentialsRequestMessage;
import org.fixtrading.silverflash.auth.proxy.CredentialsResponseMessage;
import org.fixtrading.silverflash.fixp.Engine;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.auth.AbstractAuthenticator;
import org.fixtrading.silverflash.fixp.auth.TicketAuthenticator;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.bridge.EventReactorWithBridge;
import org.fixtrading.silverflash.transport.SharedMemoryTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Don Mendelson
 *
 */
@Ignore
public class TicketAuthenticatorTest {

  private AuthenticationServer authenticationServer;
  private TicketAuthenticator ticketAuthenticator;
  private Engine serverEngine;
  private Engine clientEngine;

  private InetAddress host = InetAddress.getLoopbackAddress();
  private int port = 5234;
  private AuthenticationProxy proxy;
  private Entity entity = new Entity("user1");
  private char[] password = "password1".toCharArray();
  private char[] storePassphrase = "password1".toCharArray();
  private EventReactorWithBridge clientReactor;
  private Topic requestTopic = SessionEventTopics.getTopic(SERVICE_CREDENTIALS_REQUEST);
  private Topic responseTopic = SessionEventTopics.getTopic(SERVICE_CREDENTIALS_RETRIEVED);

  class CredentialReceiver implements Receiver {

    private CredentialsResponseMessage responseMessage = new CredentialsResponseMessage();

    public String responseName = null;
    public byte[] responseTicket = null;

    public void accept(ByteBuffer buffer) {
      responseMessage.attachForDecode(buffer, 0);
      try {
        responseName = responseMessage.getName();
        responseTicket = responseMessage.getTicket();
      } catch (UnsupportedEncodingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

  }

  @Before
  public void setUp() throws Exception {
    serverEngine = Engine.builder().build();
    Directory directory = new SimpleDirectory();
    directory.add(entity.getName());
    directory.setProperty(entity.getName(), Directory.PW_DIRECTORY_ATTR, password);
    
    String distinguishedNames = "CN=trading, O=myorg, C=US";

    serverEngine.open();
    Transport serverPipe = new SharedMemoryTransport(false, 1, true);
    authenticationServer = new AuthenticationServer(host, port, storePassphrase, distinguishedNames, serverPipe);
    CompletableFuture<AuthenticationServer> future1 = authenticationServer.open();

    EventReactorWithBridge serverReactor =
        EventReactorWithBridge.builder().withTransport(serverPipe)
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    CompletableFuture<? extends EventReactor<ByteBuffer>> future2 = serverReactor.open();

    ticketAuthenticator = new TicketAuthenticator();
    ticketAuthenticator.withEventReactor(serverReactor);
    CompletableFuture<? extends AbstractAuthenticator> future3 = ticketAuthenticator.open();

    clientEngine = Engine.builder().build();
    clientEngine.open();

    Transport clientPipe = new SharedMemoryTransport(true, 2, true);
    clientReactor =
        EventReactorWithBridge.builder().withTransport(clientPipe)
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    CompletableFuture<? extends EventReactor<ByteBuffer>> future4 = clientReactor.open();
    // clientReactor.setTrace(true, "clientReactor");
    clientReactor.forward(requestTopic);

    Transport proxyPipe = new SharedMemoryTransport(false, 2, true);
    proxy = new AuthenticationProxy(host, port, proxyPipe, MockCrypto.getTestTrustManager());
    CompletableFuture<AuthenticationProxy> future5 = proxy.open();

    CompletableFuture.allOf(future1, future2, future3, future4, future5).get();
  }

  @After
  public void tearDown() throws Exception {
    serverEngine.close();
    clientEngine.close();
    ticketAuthenticator.close();
  }

  @Test
  public void authenticate() throws Exception {
    CredentialReceiver clientCredentialReceiver = new CredentialReceiver();
    Subscription responseSubscription =
        clientReactor.subscribe(responseTopic, clientCredentialReceiver);

    CredentialsRequestMessage requestMessage = new CredentialsRequestMessage();
    ByteBuffer buffer = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
    requestMessage.attachForEncode(buffer, 0);
    requestMessage.setName(entity.getName());
    requestMessage.setPassword(password);
    clientReactor.post(requestTopic, buffer);

    Thread.sleep(3000L);

    assertEquals(entity.getName(), clientCredentialReceiver.responseName);
    assertNotNull(clientCredentialReceiver.responseTicket);

    byte[] credentials = new byte[1024];

    int length = Credentials.encode(entity, clientCredentialReceiver.responseTicket, credentials);
    assertTrue(ticketAuthenticator.authenticate(SessionId.generateUUID(),
        Arrays.copyOf(credentials, length)));
  }

}
