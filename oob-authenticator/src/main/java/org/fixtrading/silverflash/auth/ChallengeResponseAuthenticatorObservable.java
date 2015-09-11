package org.fixtrading.silverflash.auth;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.*;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadFactory;

import org.fixtrading.silverflash.auth.Directory;
import org.fixtrading.silverflash.auth.proxy.CredentialsResponseMessage;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * An extension of ChallengeResponseAuthenticator that posts asynchronous results
 * 
 * @author Don Mendelson
 *
 */
public class ChallengeResponseAuthenticatorObservable extends ChallengeResponseAuthenticator {

  private final ByteBuffer buffer = ByteBuffer.allocateDirect(2048).order(ByteOrder.nativeOrder());
  private final EventReactor<ByteBuffer> reactor;
  private final CredentialsResponseMessage responseMessage = new CredentialsResponseMessage();
  private final Topic topic = SessionEventTopics.getTopic(SERVICE_CREDENTIALS_RETRIEVED);

  /**
   * Constructor
   * 
   * @param address listen address
   * @param port listen port
   * @param storePassphrase passphrase to access keys
   * @param distinguishedNames a string representation of an entry in a directory
   * @param directory directory of entities to authenticate
   * @param reactor an EventReactor
   */
  public ChallengeResponseAuthenticatorObservable(InetAddress address, int port,
      char[] storePassphrase, String distinguishedNames, Directory directory,
      EventReactor<ByteBuffer> reactor) {
    super(address, port, storePassphrase, distinguishedNames, directory);
    this.reactor = reactor;
  }

  /**
   * Constructor
   * 
   * @param address listen address
   * @param port listen port
   * @param storePassphrase passphrase to access keys
   * @param distinguishedNames a string representation of an entry in a directory
   * @param directory directory of entities to authenticate
   * @param threadFactory supplies threads to run server
   * @param reactor an EventReactor
   */
  public ChallengeResponseAuthenticatorObservable(InetAddress address, int port,
      char[] storePassphrase, String distinguishedNames, Directory directory,
      ThreadFactory threadFactory, EventReactor<ByteBuffer> reactor) {
    super(address, port, storePassphrase, distinguishedNames, directory, threadFactory);
    this.reactor = reactor;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.auth.ChallengeResponseAuthenticator#onAuthenticated(java
   * .lang.String, byte[])
   */
  @Override
  protected synchronized void onAuthenticated(String name, byte[] ticket) {

    try {
      buffer.clear();
      responseMessage.attachForEncode(buffer, 0);
      responseMessage.setName(name);
      responseMessage.setTicket(ticket);
      reactor.post(topic, buffer);
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
