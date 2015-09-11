package org.fixtrading.silverflash.reactor;

/**
 * Key for event publish / subscribe
 * <p>
 * Implementations should be immutable
 * 
 * @author Don Mendelson
 *
 */
public interface Topic extends Comparable<Topic> {

  /**
   * Returns an ordered list of names that identifies this Topic
   * 
   * @return an array of names
   */
  String[] getFields();

  /**
   * Tests whether the other Topic is a subtopic of this Topic
   * 
   * @param other the Topic to test
   * @return Returns {@code true} if the other Topic is a subtopic
   */
  boolean isSubtopic(Topic other);
}
