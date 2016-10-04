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

package io.fixprotocol.silverflash.fixp.store;

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
