package org.fixtrading.silverflash.fixp.auth;

import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * @author Don Mendelson
 *
 */
public final class MockCrypto {

  /**
   * For testing only!!!
   * 
   * @return a TrustManager that accepts all certs
   */
  public static TrustManager getTestTrustManager() {
    return new X509TrustManager() {
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }

      public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}

      public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
    };
  }
}
