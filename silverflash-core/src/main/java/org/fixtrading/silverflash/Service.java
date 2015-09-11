package org.fixtrading.silverflash;

import java.util.concurrent.CompletableFuture;

/**
 * A background process that provides services
 * 
 * @author Don Mendelson
 *
 */
public interface Service extends AutoCloseable {

  /**
   * Start this Service
   * 
   * @return a CompletableFuture that asynchronously provides a reference to this Service upon
   *         successful completion or an exception if it fails to start
   */
  CompletableFuture<? extends Service> open();
}
