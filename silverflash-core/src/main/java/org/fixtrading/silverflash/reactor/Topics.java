/**
 *    Copyright 2015 FIX Protocol Ltd
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

package org.fixtrading.silverflash.reactor;

import java.util.Arrays;
import java.util.Objects;

/**
 * Topic operators
 * 
 * @author Don Mendelson
 *
 */
public final class Topics {
  private static class TopicImpl implements Topic {

    private static final String DELIMITER = "/";

    private final String[] fields;
    private final int[] parts;

    public TopicImpl(String[] fields) {
      this.fields = fields;
      parts = new int[fields.length];
      for (int i = 0; i < fields.length; i++) {
        parts[i] = fields[i].hashCode();
      }
    }

    @Override
    public int compareTo(Topic obj) {
      Objects.requireNonNull(obj);
      if (getClass() != obj.getClass()) {
        return 1;
      }
      TopicImpl other = (TopicImpl) obj;

      int result = 0;
      int leastParts = Math.min(parts.length, other.parts.length);
      for (int i = 0; result == 0 && i < leastParts; i++) {
        result = parts[i] - other.parts[i];
      }
      if (result == 0) {
        result = parts.length - other.parts.length;
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TopicImpl other = (TopicImpl) obj;
      return Arrays.equals(parts, other.parts);
    }

    public String[] getFields() {
      return fields;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(parts);
      return result;
    }

    public boolean isSubtopic(Topic obj) {
      if (getClass() != obj.getClass()) {
        return false;
      }
      TopicImpl other = (TopicImpl) obj;

      if (other.parts.length < parts.length) {
        return false;
      }

      for (int i = 0; i < parts.length; i++) {
        if (parts[i] != other.parts[i]) {
          return false;
        }
      }

      return true;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      for (int i = 0; i < fields.length; i++) {
        b.append(DELIMITER);
        b.append(fields[i]);
      }
      return b.toString();
    }

    public static TopicImpl parse(String string) {
      String[] f =
          Arrays.stream(string.split(DELIMITER)).filter(s -> s.length() > 0).toArray(String[]::new);
      return new TopicImpl(f);
    }

  }

  /**
   * Returns a new Topic as subtopic
   * 
   * @param topic the base Topic
   * @param field an additional qualifier
   * @return
   */
  public static Topic getSubtopic(Topic topic, String field) {
    String[] baseFields = topic.getFields();
    String[] fields = new String[baseFields.length + 1];
    System.arraycopy(baseFields, 0, fields, 0, baseFields.length);
    fields[baseFields.length] = field;
    return new TopicImpl(fields);
  }

  /**
   * Returns a new Topic with a given name
   * 
   * @param name identifier
   * @return a new Topic
   */
  public static Topic getTopic(String name) {
    return new TopicImpl(new String[] {name});
  }

  /**
   * Returns a new Topic formed from an ordered list of identifiers
   * 
   * @param fields an array of names
   * @return a new Topic
   */
  public static Topic getTopic(String... fields) {
    return new TopicImpl(fields);
  }

  /**
   * Returns a new Topic formed from a String delimited by '/' character
   * 
   * @param s String to parse
   * @return a new Topic
   */
  public static Topic parse(String s) {
    return TopicImpl.parse(s);
  }

}
