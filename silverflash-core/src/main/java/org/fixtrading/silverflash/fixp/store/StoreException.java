package org.fixtrading.silverflash.fixp.store;

/**
 * An Exception in an operation on a MessageStore
 * 
 * @author Don Mendelson
 *
 */
public class StoreException extends Exception {

  private static final long serialVersionUID = -1168862551207198135L;

  /**
	 * 
	 */
  public StoreException() {}

  /**
   * @param message error message
   */
  public StoreException(String message) {
    super(message);
  }

  /**
   * @param cause cause of this StoreException
   */
  public StoreException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message error message
   * @param cause cause of this StoreException
   */
  public StoreException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * @param message error message
   * @param cause cause of this StoreException
   * @param enableSuppression whether or not suppression is enabled or disabled
   * @param writableStackTrace whether or not the stack trace should be writable
   */
  public StoreException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
