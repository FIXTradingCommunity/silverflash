package org.fixtrading.silverflash;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * A message consumer
 * 
 * @author Don Mendelson
 *
 */
public interface Receiver extends Consumer<ByteBuffer> {

}
