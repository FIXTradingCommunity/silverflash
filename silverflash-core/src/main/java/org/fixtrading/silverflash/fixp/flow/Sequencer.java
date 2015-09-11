package org.fixtrading.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.util.function.Function;

import org.fixtrading.silverflash.Sequenced;

/**
 * @author Don Mendelson
 *
 */
public interface Sequencer extends Function<ByteBuffer[], ByteBuffer[]>, Sequenced {

}
