package org.fixtrading.silverflash.auth;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.fixtrading.silverflash.auth.Crypto;

/**
 * Client authentication
 * <p>
 * Privacy is provided by TLS, not the SASL layer. Implementation is synchronous and not
 * thread-safe.
 * 
 * @author Don Mendelson
 *
 */
public class ChallengeResponseClient {

  private final byte[] netbytes = new byte[2048];

  private final CallbackHandler callbackHandler = new CallbackHandler() {

    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (int i = 0; i < callbacks.length; i++) {
        if (callbacks[i] instanceof NameCallback) {
          NameCallback callback = (NameCallback) callbacks[i];
          callback.setName(name);
        } else if (callbacks[i] instanceof PasswordCallback) {
          PasswordCallback callback = (PasswordCallback) callbacks[i];
          callback.setPassword(password);
        }
      }
    }

  };

  private static final byte[] EMPTY = new byte[0];
  private final InetAddress host;
  private final Object lock = new Object();
  private String name;
  private char[] password;
  private final int port;
  private SaslClient sc;

  private final HandshakeCompletedListener sslListener = event -> handshakeComplete();

  private byte[] token;

  private final TrustManager[] trustManagers;

  /**
   * Constructor
   * 
   * @param host remote host address
   * @param port remote port
   * @throws IOException if an IO error occurs
   * @throws GeneralSecurityException if KeyStore or TrustManager cannot be created
   */
  public ChallengeResponseClient(InetAddress host, int port) throws GeneralSecurityException,
      IOException {
    this.host = host;
    this.port = port;
    KeyStore truststore = Crypto.createKeyStore();
    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(truststore);
    this.trustManagers = tmf.getTrustManagers();
  }

  /**
   * Constructor
   * 
   * @param host remote host address
   * @param port remote port
   * @param trustManager to store trusted keys
   */
  public ChallengeResponseClient(InetAddress host, int port, TrustManager trustManager) {
    this.host = host;
    this.port = port;
    this.trustManagers = new TrustManager[1];
    this.trustManagers[0] = trustManager;
  }

  /**
   * Authenticate an entity
   * 
   * @param name name of entity
   * @param password secret password
   * @return Returns {@code true} if the entity was successfully authenticated
   * @throws IOException if an IO error occurs
   * @throws InterruptedException if the protocol is interrupted before completion
   * @throws GeneralSecurityException if authentication fails
   */
  public boolean authenticate(String name, char[] password) throws IOException,
      InterruptedException, GeneralSecurityException {
    this.name = name;
    this.password = password;

    boolean success = false;

    SSLContext sslContext = initializeSSLContext();
    sc = initializeSaslClient();

    SSLSocket socket = (SSLSocket) sslContext.getSocketFactory().createSocket(host, port);
    socket.setUseClientMode(true);

    try {
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      socket.addHandshakeCompletedListener(sslListener);

      socket.startHandshake();
      waitForHandshakeCompletion(5000L);

      // Get optional initial response
      byte[] response = (sc.hasInitialResponse() ? sc.evaluateChallenge(EMPTY) : EMPTY);

      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());

      // Send initial response to server
      send(out, response);

      byte[] challenge = new byte[16 * 1024];

      while (!sc.isComplete()) {
        // Read response
        int bytesRead = receive(in, challenge);
        if (bytesRead < 0) {
          break;
        }
        // Evaluate server challenge
        response = sc.evaluateChallenge(Arrays.copyOf(challenge, bytesRead));

        if (response != null) {
          send(out, response);
        }
      }
      success = sc.isComplete();

      byte[] tokenBuffer = new byte[2048];
      int tokenLength = 0;
      if (success) {
        if (isPrivate()) {
          tokenLength = receiveSecure(in, tokenBuffer);
        } else {
          tokenLength = receive(in, tokenBuffer);
        }
      }
      this.token = Arrays.copyOf(tokenBuffer, tokenLength);

    } finally {
      sc.dispose();
      socket.close();
    }
    return success;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ChallengeResponseClient [");
    if (host != null) {
      builder.append("host=");
      builder.append(host);
      builder.append(", ");
    }
    builder.append("port=");
    builder.append(port);
    builder.append("]");
    return builder.toString();
  }

  /**
   * @return the token
   */
  public byte[] getToken() {
    return token;
  }

  private void handshakeComplete() {
    synchronized (lock) {
      lock.notify();
    }
  }

  private SaslClient initializeSaslClient() throws IOException {
    // CRAM-MD5 supports a hashed username/password authentication scheme.
    // CRAM-MD5 uses NameCallback and PasswordCallback

    // Would like to use SCRAM, but not yet supported by Java
    String[] mechanisms = new String[] {"CRAM-MD5"};
    String protocol = null;
    // Used by CRAM-MD5 as default user name
    String authorizationId = null;
    String serverName = null;
    Map<String, Object> props = new HashMap<>();
    props.put(Sasl.POLICY_NOPLAINTEXT, "true");
    props.put(Sasl.POLICY_NOANONYMOUS, "true");
    // Note: callbacks get called immediately, so data must be set prior to
    // this point
    sc =
        Sasl.createSaslClient(mechanisms, authorizationId, protocol, serverName, props,
            callbackHandler);
    return sc;
  }

  private SSLContext initializeSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagers, null);
    return sslContext;
  }

  private boolean isPrivate() {
    String qop = (String) sc.getNegotiatedProperty(Sasl.QOP);
    return (qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf")));
  }

  private int receive(DataInputStream in, byte[] incoming) throws IOException {
    int bytesRead = in.readInt();
    in.read(incoming, 0, bytesRead);
    return bytesRead;
  }

  private int receiveSecure(DataInputStream in, byte[] incoming) throws IOException {
    int bytesRead = in.read(netbytes);
    byte[] decoded = sc.unwrap(netbytes, 0, bytesRead);
    System.arraycopy(decoded, 0, incoming, 0, decoded.length);
    return decoded.length;
  }

  private void send(DataOutputStream out, byte[] outgoing) throws IOException {
    out.writeInt(outgoing.length);
    out.write(outgoing);
  }

  private void sendSecure(DataOutputStream out, byte[] outgoing) throws IOException {
    byte[] bytes = sc.wrap(outgoing, 0, outgoing.length);
    out.write(bytes);
  }

  private void waitForHandshakeCompletion(long timeout) throws InterruptedException {
    synchronized (lock) {
      lock.wait(timeout);
    }
  }

}
