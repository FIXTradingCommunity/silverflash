package org.fixtrading.silverflash.fixp.auth;

import org.fixtrading.silverflash.auth.Authenticator;
import org.fixtrading.silverflash.reactor.Reactive;

/**
 * An Authenticator that reacts asynchronously
 * 
 * @author Don Mendelson
 *
 * @param <T> type of session ID
 * @param <U> type of event payload
 */
public interface ReactiveAuthenticator<T, U> extends Authenticator<T>, Reactive<U> {

}
