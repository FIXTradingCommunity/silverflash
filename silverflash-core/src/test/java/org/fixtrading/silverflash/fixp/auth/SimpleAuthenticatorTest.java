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

import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.fixtrading.silverflash.auth.Directory;
import org.fixtrading.silverflash.auth.SimpleDirectory;
import org.fixtrading.silverflash.fixp.Engine;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.auth.SimpleAuthenticator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Don Mendelson
 *
 */
public class SimpleAuthenticatorTest {

  private Engine engine = Engine.builder().build();
  private SimpleAuthenticator authenticator;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {

    Directory directory = new SimpleDirectory();
    directory.add("user1");

    engine.open();
    authenticator = new SimpleAuthenticator().withDirectory(directory);
    authenticator.withEventReactor(engine.getReactor());
    authenticator.open().get();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    authenticator.close();
    engine.close();
  }

  /**
   * Test method for
   * {@link org.fixtrading.silverflash.fixp.auth.SimpleAuthenticator#authenticate(byte[], byte[])} .
   */
  @Test
  public void authenticate() {
    byte[] user1 = "user1".getBytes();
    byte[] credentials = new byte[64];
    System.arraycopy(user1, 0, credentials, 0, user1.length);

    UUID sessionId = SessionId.generateUUID();
    assertTrue(authenticator.authenticate(sessionId, credentials));
  }

  @Test
  public void authenticateFail() {
    byte[] user2 = "user2".getBytes();
    byte[] credentials = new byte[64];
    System.arraycopy(user2, 0, credentials, 0, user2.length);

    UUID sessionId = SessionId.generateUUID();
    assertTrue(!authenticator.authenticate(sessionId, credentials));
  }

}
