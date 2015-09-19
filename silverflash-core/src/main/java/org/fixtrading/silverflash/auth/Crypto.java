/**
 *    Copyright 2015 FIX Protocol Ltd
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
package org.fixtrading.silverflash.auth;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;

import sun.security.tools.keytool.CertAndKeyGen;
import sun.security.x509.X500Name;

/**
 * Methods for handling cryptographic keys
 * 
 * @author Don Mendelson
 *
 */
@SuppressWarnings("restriction")
public final class Crypto {

  /**
   * Create a Java key store
   * 
   * @return a new key store
   * @throws GeneralSecurityException if the key store cannot be instantiated
   * @throws IOException if the store cannot be read
   */
  public static KeyStore createKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null);
    return ks;
  }

  /**
   * Load a Java key store from a file or other input stream
   * 
   * @param stream input stream
   * @param password byte array containing password for key store
   * @return a populated key store
   * @throws GeneralSecurityException if the key store cannot be instantiated
   * @throws IOException if the store cannot be read
   */
  public static KeyStore loadKeyStore(InputStream stream, char[] password)
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(stream, password);
    return ks;
  }

  /**
   * Storea a Java key store to a file or other output stream
   * 
   * @param stream output stream
   * @param password byte array containing password for key store
   * @return store that was created
   * @throws GeneralSecurityException if the key store cannot be instantiated
   * @throws IOException if the store cannot be written
   */
  public static KeyStore storeKeyStore(OutputStream stream, char[] password)
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.store(stream, password);
    return ks;
  }

  /**
   * Add a certificate to a key store
   * @param ksKeys a key store
   * @param alias an alias for the certificate
   * @param distinguishedNames a string representation of an entry in a directory
   * @param keyPass password for this key
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static void addKeyCertificateEntry(KeyStore ksKeys, String alias,
      String distinguishedNames, char[] keyPass) throws GeneralSecurityException, IOException {
    // The SunEC provider implements Elliptical Curve Cryptography (ECC)
    CertAndKeyGen keyGen = new CertAndKeyGen("EC", "SHA512withECDSA", "SunEC");
    keyGen.generate(256);
    PrivateKey privKey = keyGen.getPrivateKey();

    // Generate self signed certificate
    X509Certificate[] chain = new X509Certificate[1];
    // validity in seconds
    chain[0] = keyGen.getSelfCertificate(new X500Name(distinguishedNames), (long) 365 * 24 * 3600);

    ksKeys.setKeyEntry(alias, privKey, keyPass, chain);
  }

  /**
   * Add Triple DES key
   * 
   * @param ksKeys a key store
   * @param alias alias for key entry
   * @param secretKey key to add
   * @param keyPass password for key store
   * @throws GeneralSecurityException if key cannot be added to the store
   */
  public static void addSecretKey(KeyStore ksKeys, String alias, byte[] secretKey, char[] keyPass)
      throws GeneralSecurityException {
    DESedeKeySpec keySpec = new DESedeKeySpec(secretKey);
    SecretKeyFactory factory = SecretKeyFactory.getInstance("DESede");
    SecretKey mySecretKey = factory.generateSecret(keySpec);
    KeyStore.SecretKeyEntry skEntry = new KeyStore.SecretKeyEntry(mySecretKey);
    KeyStore.ProtectionParameter protParam = new KeyStore.PasswordProtection(keyPass);
    ksKeys.setEntry(alias, skEntry, protParam);
  }

}
