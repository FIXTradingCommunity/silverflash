package org.fixtrading.silverflash.transport;

import java.util.function.Function;


/**
 * Binds a session to a newly accepted Transport
 * 
 * @author Don Mendelson
 *
 * @param T session type
 */
public class TransportSessionWrapper<T> {

  private final Acceptor acceptor;

  TransportSessionWrapper(Acceptor acceptor, Function<Transport, T> func) {
    this.acceptor = acceptor;

  }

}
