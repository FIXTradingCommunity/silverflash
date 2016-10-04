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

package io.fixprotocol.silverflash.reactor;

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
