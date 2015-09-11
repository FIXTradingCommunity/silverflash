package org.fixtrading.silverflash.fixp.auth;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.fixtrading.silverflash.auth.Credentials;
import org.fixtrading.silverflash.auth.Directory;
import org.fixtrading.silverflash.auth.Entity;

/**
 * 
 * 
 * @author Don Mendelson
 *
 */
public class SimpleAuthenticator extends AbstractAuthenticator {

  private static final String CONFIG_NAME = "directory";

  /**
   * CallbackHandler is invoked by DirectoryLoginModule via JAAS configuration.
   */
  private final CallbackHandler callbackHandler = new CallbackHandler() {

    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (int i = 0; i < callbacks.length; i++) {
        if (callbacks[i] instanceof SessionIdCallback) {
          SessionIdCallback callback = (SessionIdCallback) callbacks[i];
          callback.setSessionId(sessionId);
        } else if (callbacks[i] instanceof NameCallback) {
          NameCallback callback = (NameCallback) callbacks[i];
          callback.setName(entity.getName());
        } else if (callbacks[i] instanceof DirectoryCallback) {
          DirectoryCallback callback = (DirectoryCallback) callbacks[i];
          callback.setDirectory(directory);
        }
      }
    }
  };


  private UUID sessionId;
  private Entity entity;
  private Directory directory;

  @Override
  public boolean authenticate(UUID sessionId, byte[] credentials) {

    this.entity = Credentials.getEntity(credentials);
    this.sessionId = sessionId;
    Subject subject = new Subject();

    try {
      System.setProperty("java.security.auth.login.config", "jaas-directory.config");
      LoginContext context = new LoginContext(CONFIG_NAME, subject, this.callbackHandler);
      context.login();
      System.out.format("Authenticated session ID=%s credentials=%s\n", sessionId.toString(),
          entity.getName());
      return true;
    } catch (LoginException e) {
      System.out.format("Authentication failed for session ID=%s credentials=%s; %s\n",
          sessionId.toString(), entity.getName(), e.getMessage());
      return false;
    }

  }

  public SimpleAuthenticator withDirectory(Directory directory) {
    Objects.requireNonNull(directory);
    this.directory = directory;
    return this;
  }
}
