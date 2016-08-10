/**
 *    Copyright 2015-2016 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.fixtrading.silverflash.fixp.auth;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.fixtrading.silverflash.auth.Directory;

/**
 * Authentication using JAAS
 * 
 * @author Don Mendelson
 *
 */
public class DirectoryLoginModule implements LoginModule {

  private final Set<UUID> sessionIds = new CopyOnWriteArraySet<>();
  private Subject subject;
  private CallbackHandler callbackHandler;
  private Map<String, ?> sharedState;
  private Map<String, ?> options;
  private UUID uuid;
  private String name;
  private Directory directory;

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#initialize(javax.security.auth.Subject ,
   * javax.security.auth.callback.CallbackHandler, java.util.Map, java.util.Map)
   */
  public void initialize(Subject subject, CallbackHandler callbackHandler,
      Map<String, ?> sharedState, Map<String, ?> options) {
    this.subject = subject;
    this.callbackHandler = callbackHandler;
    this.sharedState = sharedState;
    this.options = options;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#login()
   */
  public boolean login() throws LoginException {

    final Callback[] callbacks = new Callback[3];
    final DirectoryCallback directoryCallback = new DirectoryCallback();
    final SessionIdCallback sessionIdCallback = new SessionIdCallback();
    final NameCallback nameCallback = new NameCallback("Noprompt");
    callbacks[0] = directoryCallback;
    callbacks[1] = sessionIdCallback;
    callbacks[2] = nameCallback;

    try {
      callbackHandler.handle(callbacks);
    } catch (IOException | UnsupportedCallbackException e) {
      throw new LoginException(e.getMessage());
    }

    this.directory = directoryCallback.getDirectory();
    this.uuid = sessionIdCallback.getSessionId();

    if (uuid == null || !sessionIds.add(uuid)) {
      throw new LoginException("Missing or duplicate session ID");
    }

    this.name = nameCallback.getName();

    if (name == null || name.length() == 0 || !isValidName(name)) {
      throw new LoginException("Invalid or empty entity name");
    }

    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#commit()
   */
  public boolean commit() throws LoginException {
    subject.getPrincipals().add(new SessionPrincipal(uuid));
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#abort()
   */
  public boolean abort() throws LoginException {
    sessionIds.remove(uuid);
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#logout()
   */
  public boolean logout() throws LoginException {
    Set<SessionPrincipal> sessionPrincipals = subject.getPrincipals(SessionPrincipal.class);
    for (SessionPrincipal principal : sessionPrincipals) {
      subject.getPrincipals().remove(principal);
      sessionIds.remove(principal.getUUID());
    }

    return true;
  }


  private boolean isValidName(String name) {
    return directory != null && directory.isPresent(name);
  }

}
