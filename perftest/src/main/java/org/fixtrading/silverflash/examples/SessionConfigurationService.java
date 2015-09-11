package org.fixtrading.silverflash.examples;


/**
 * Configuration for sessions
 * 
 * @author Don Mendelson
 *
 */
public interface SessionConfigurationService {

  boolean isOutboundFlowRecoverable();

  boolean isOutboundFlowSequenced();

  int getKeepaliveInterval();

  byte[] getCredentials();

  boolean isTransportMultiplexed();
}
