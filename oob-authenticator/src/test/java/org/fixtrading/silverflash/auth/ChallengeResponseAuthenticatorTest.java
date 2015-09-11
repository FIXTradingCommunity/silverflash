package org.fixtrading.silverflash.auth;

import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;

import org.fixtrading.silverflash.auth.ChallengeResponseAuthenticator;
import org.fixtrading.silverflash.auth.ChallengeResponseClient;
import org.fixtrading.silverflash.auth.Directory;
import org.fixtrading.silverflash.auth.SimpleDirectory;
import org.fixtrading.silverflash.fixp.auth.MockCrypto;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Don Mendelson
 *
 */
public class ChallengeResponseAuthenticatorTest {

  ChallengeResponseAuthenticator server;
  ChallengeResponseClient client;
  String name = "testUser";
  char[] password = "mySecretPassword".toCharArray();

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    int port = 4567;
    InetAddress address = InetAddress.getLoopbackAddress();
    char[] storePassphrase = "myPassphrase".toCharArray();
    String distinguishedNames = "CN=trading, O=myorg, C=US";
    
    Directory directory = new SimpleDirectory();
    directory.add(name);
    directory.setProperty(name, Directory.PW_DIRECTORY_ATTR, password);

    server = new ChallengeResponseAuthenticator(address, port, storePassphrase, distinguishedNames, directory);
    client = new ChallengeResponseClient(address, port, MockCrypto.getTestTrustManager());
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    server.close();
  }

  @Test
  public void authenticate() throws Exception {
    CompletableFuture<ChallengeResponseAuthenticator> future = server.open();
    future.get();
    assertTrue(client.authenticate(name, password));
  }

}
