package org.fixtrading.silverflash.auth;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.auth.Crypto;
import org.fixtrading.silverflash.auth.Directory;

/**
 * Authentication server
 * <p>
 * Privacy is provided by TLS, not the SASL layer. Implementation is thread per client policy, but
 * connections are expected to be short-lived.
 * 
 * @author Don Mendelson
 *
 */
public class ChallengeResponseAuthenticator implements Service, Runnable {

  private static final String CERTIFICATE_ALIAS = "mycert";

  private class ClientHandler implements Runnable {

    private final CallbackHandler callbackHandler = new CallbackHandler() {

      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
          if (callbacks[i] instanceof NameCallback) {
            NameCallback callback = (NameCallback) callbacks[i];
            name = callback.getDefaultName();
          } else if (callbacks[i] instanceof PasswordCallback) {
            PasswordCallback callback = (PasswordCallback) callbacks[i];
            char[] pw = getPasswordForUser(name);
            callback.setPassword(pw);
          } else if (callbacks[i] instanceof AuthorizeCallback) {
            AuthorizeCallback callback = (AuthorizeCallback) callbacks[i];
            String authid = callback.getAuthenticationID();
            String authzid = callback.getAuthorizationID();
            if (authid.equals(authzid)) {
              callback.setAuthorized(true);
              callback.setAuthorizedID(authzid);
            } else {
              callback.setAuthorized(false);
            }
          }
        }
      }

    };

    private final Object lock = new Object();

    private String name;
    private final byte[] netbytes = new byte[2048];
    private final SSLSocket socket;
    private SaslServer ss;

    private final HandshakeCompletedListener sslListener = arg0 -> handshakeComplete();

    /**
     * Constructor
     * 
     * @param socket new SSL socket
     */
    public ClientHandler(SSLSocket socket) {
      this.socket = socket;
      Thread thread = new Thread(this);
      thread.setDaemon(true);
      thread.start();

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
      try {
        SaslServer saslServer = initialize();
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        socket.addHandshakeCompletedListener(sslListener);

        socket.startHandshake();

        long timeout = 1000L;
        waitForHandshakeCompletion(timeout);

        byte[] response = new byte[16 * 1024];

        while (!saslServer.isComplete()) {
          // Read response
          int bytesRead = receive(in, response);
          if (bytesRead < 0) {
            break;
          }
          // Evaluate server challenge
          byte[] challenge = saslServer.evaluateResponse(Arrays.copyOf(response, bytesRead));
          if (challenge != null) {
            send(out, challenge);
          }
        }

        if (saslServer.isComplete()) {
          String authzId = saslServer.getAuthorizationID();

          byte[] token = Ticket.generateTicket(random, authzId);
          onAuthenticated(name, token);

          if (isPrivate()) {
            sendSecure(out, token);
          } else {
            send(out, token);
          }
        }

        // Wait for client to break connection
        try {
          receive(in, response);
        } catch (IOException ex) {
          // ignore EOF
        }

      } catch (IOException | InterruptedException e) {
        exceptionConsumer.accept(e);
      } finally {
        cleanup();
      }
    }

    private void cleanup() {
      if (ss != null) {
        try {
          ss.dispose();
        } catch (SaslException e) {
          exceptionConsumer.accept(e);
        }
      }

      try {
        socket.close();
      } catch (IOException e) {
        exceptionConsumer.accept(e);
      }
    }

    private void handshakeComplete() {
      synchronized (lock) {
        lock.notify();
      }
    }

    private SaslServer initialize() throws SaslException {
      // CRAM-MD5 supports a hashed username/password authentication
      // scheme.
      // CRAM-MD5 server uses AuthorizeCallback, NameCallback and
      // PasswordCallback

      // Would like to use SCRAM, but not yet supported by Java
      String mechanism = "CRAM-MD5";
      String protocol = "fixp";
      // Used by CRAM-MD5 as default user name
      String authorizationId = null;
      String serverName = null;
      Map<String, Object> props = new HashMap<>();
      props.put(Sasl.POLICY_NOPLAINTEXT, "true");
      props.put(Sasl.POLICY_NOANONYMOUS, "true");
      ss =
          Sasl.createSaslServer(mechanism, protocol, fullyQualifiedDomainName, props,
              callbackHandler);
      return ss;
    }

    private boolean isPrivate() {
      String qop = (String) ss.getNegotiatedProperty(Sasl.QOP);
      return (qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf")));
    }

    private int receive(DataInputStream in, byte[] incoming) throws IOException {
      int bytesRead = in.readInt();
      in.read(incoming, 0, bytesRead);
      return bytesRead;
    }

    private int receiveSecure(DataInputStream in, byte[] incoming) throws IOException {
      int bytesRead = in.read(netbytes);
      byte[] decoded = ss.unwrap(netbytes, 0, bytesRead);
      System.arraycopy(decoded, 0, incoming, 0, decoded.length);
      return decoded.length;
    }

    private void send(DataOutputStream out, byte[] outgoing) throws IOException {
      out.writeInt(outgoing.length);
      out.write(outgoing);
    }

    private void sendSecure(DataOutputStream out, byte[] outgoing) throws IOException {
      byte[] bytes = ss.wrap(outgoing, 0, outgoing.length);
      out.write(bytes);
    }

    private void waitForHandshakeCompletion(long timeout) throws InterruptedException {
      synchronized (lock) {

        lock.wait(timeout);
      }
    }
  }

  private final InetAddress address;

  private int backlog = 5;
  private final Directory directory;
  private ExceptionConsumer exceptionConsumer = ex -> {
    System.err.println(ex);
  };
  private final String fullyQualifiedDomainName;
  private CompletableFuture<ChallengeResponseAuthenticator> future;
  private int port = 0;
  private final SecureRandom random;
  private final AtomicBoolean running = new AtomicBoolean();
  private SSLServerSocket serverSocket;
  private Thread serverThread;
  private SSLContext sslContext;
  private final char[] storePassphrase;
  private final ThreadFactory threadFactory;
  private final String distinguishedNames;

  /**
   * Constructor
   * 
   * @param address listen address
   * @param port listen port
   * @param storePassphrase passphrase to access keys
   * @param distinguishedNames 
   * @param directory directory of entities to authenticate
   */
  public ChallengeResponseAuthenticator(InetAddress address, int port, char[] storePassphrase,
      String distinguishedNames, Directory directory) {
    this(address, port, storePassphrase, distinguishedNames, directory, Executors
        .defaultThreadFactory());
  }

  /**
   * Constructor
   * 
   * @param address listen address
   * @param port listen port
   * @param storePassphrase passphrase to access keys
   * @param distinguishedNames 
   * @param directory directory of entities to authenticate
   * @param threadFactory provides threads to run a server
   */
  public ChallengeResponseAuthenticator(InetAddress address, int port, char[] storePassphrase,
      String distinguishedNames, Directory directory, ThreadFactory threadFactory) {
    Objects.requireNonNull(directory);
    this.address = address;
    this.port = port;
    this.storePassphrase = storePassphrase;
    this.distinguishedNames = distinguishedNames;
    this.directory = directory;
    this.threadFactory = threadFactory;
    this.fullyQualifiedDomainName = address.getCanonicalHostName();
    // Consider configuration for a provider
    this.random = new SecureRandom();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.AutoCloseable#close()
   */
  public void close() throws Exception {
    if (running.compareAndSet(true, false)) {
      if (serverSocket != null) {
        serverSocket.close();
      }
      if (serverThread != null) {
        try {
          serverThread.join(1000L);
        } catch (InterruptedException e) {

        }
      }
    }
  }

  public boolean isRunning() {
    return running.get();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Service#open()
   */
  public CompletableFuture<ChallengeResponseAuthenticator> open() {
    if (running.compareAndSet(false, false)) {
      future = new CompletableFuture<>();
      KeyManagerFactory kmf;
      try {
        kmf = KeyManagerFactory.getInstance("SunX509");

        KeyStore keystore = Crypto.createKeyStore();
        Crypto.addKeyCertificateEntry(keystore, CERTIFICATE_ALIAS, distinguishedNames,
            storePassphrase);
        kmf.init(keystore, storePassphrase);

        sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), null, null);

        serverThread = threadFactory.newThread(this);
        serverThread.start();
      } catch (Exception ex) {
        future.completeExceptionally(ex);
      }
    } else {
      return CompletableFuture.completedFuture(this);
    }
    return future;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Runnable#run()
   */
  public void run() {
    try {
      if (running.compareAndSet(false, true)) {

        SSLServerSocketFactory socketFactory = sslContext.getServerSocketFactory();
        serverSocket = (SSLServerSocket) socketFactory.createServerSocket(port, backlog, address);
      }
      future.complete(this);
    } catch (IOException ex) {
      future.completeExceptionally(ex);
    }

    while (running.compareAndSet(true, true)) {
      try {
        SSLSocket socket = (SSLSocket) serverSocket.accept();
        // Auth is in SASL, not TLS
        socket.setWantClientAuth(false);

        new ClientHandler(socket);
      } catch (IOException e) {
        exceptionConsumer.accept(e);
      } finally {
        running.set(false);
      }
    }
  }

  private char[] getPasswordForUser(String name) {
    char[] pw = (char[]) directory.getProperty(name, Directory.PW_DIRECTORY_ATTR);
    if (pw != null) {
      return pw;
    } else {
      return new char[0];
    }
  }

  protected void onAuthenticated(String name, byte[] ticket) {

  }
}
