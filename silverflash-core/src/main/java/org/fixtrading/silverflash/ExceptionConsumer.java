package org.fixtrading.silverflash;

import java.util.function.Consumer;

/**
 * A handler for exceptions received in inner contexts
 * <p>
 * Implementations may attempt cleanup, perform logging, etc. without breaking flow.
 * 
 * @author E16244
 *
 */
public interface ExceptionConsumer extends Consumer<Exception> {

}
