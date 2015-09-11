package org.fixtrading.silverflash.util;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Prints the contents of a buffer in hex and ASCII
 * 
 * @author Don Mendelson
 * 
 */
public final class BufferDumper {

  /**
   * Print a byte array as hex and ASCII
   * 
   * @param bytes data to print
   * @param index index from start of data buffer to print
   * @param width width of output as number of bytes of file to print per line
   * @param messageSize maximum number of bytes to print
   * @param out output stream
   * @throws UnsupportedEncodingException if character set conversion fails
   */
  public static void print(byte[] bytes, int index, int width, long messageSize, PrintStream out)
      throws UnsupportedEncodingException {
    long length = Math.min(bytes.length - index, messageSize);
    for (int i = 0; i < length; i += width) {
      printHex(bytes, index + i, width, out);
      printAscii(bytes, index + i, width, out);
    }
  }

  /**
   * Print a byte array as hex and ASCII
   * 
   * @param bytes data to print
   * @param width width of output as number of bytes of file to print per line
   * @param messageSize maximum number of bytes to print
   * @param out output stream
   * @throws UnsupportedEncodingException if character set conversion fails
   */
  public static void print(byte[] bytes, int width, long messageSize, PrintStream out)
      throws UnsupportedEncodingException {
    print(bytes, 0, width, messageSize, out);
  }

  /**
   * Print a byte array as hex and ASCII
   * 
   * @param buffer buffer to print
   * @param width width of output as number of bytes of file to print per line
   * @param out output stream
   * @throws UnsupportedEncodingException if character set conversion fails
   */
  public static void print(ByteBuffer buffer, int width, PrintStream out)
      throws UnsupportedEncodingException {
    ByteBuffer buffer2 = buffer.duplicate();
    buffer2.order(buffer.order());
    if (buffer2.position() == buffer2.limit()) {
      buffer2.rewind();
    }
    byte[] bytes = new byte[buffer2.remaining()];
    buffer2.get(bytes);
    print(bytes, 0, width, bytes.length, out);
  }

  private static void printAscii(byte[] bytes, int index, int width, PrintStream out)
      throws UnsupportedEncodingException {
    if (index < bytes.length) {
      out.println(":"
          + new String(bytes, index, Math.min(width, bytes.length - index), "UTF-8").replaceAll(
              "[^\\x20-\\x7E]", " "));
    } else {
      out.println();
    }
  }

  private static void printHex(byte[] bytes, int offset, int width, PrintStream out) {
    for (int index = 0; index < width; index++) {
      if (index + offset < bytes.length) {
        out.printf("%02x ", bytes[index + offset]);
      } else {
        out.print("  ");
      }
    }
  }

}
