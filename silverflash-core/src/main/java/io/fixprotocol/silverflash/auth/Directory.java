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

package io.fixprotocol.silverflash.auth;

/**
 * Look up entities and their attributes
 * <p>
 * Production systems may use services such as LDAP, but not committing to a particular API here.
 * 
 * @author Don Mendelson
 *
 */
public interface Directory {

  /**
   * Property name for password
   */
  String PW_DIRECTORY_ATTR = "PW_DIRECTORY_ATTR";

  /**
   * Add a new entity
   * 
   * @param name entity name
   * @return Returns {@code true} if the entity was added or {@code false} if it already existed
   */
  boolean add(String name);

  /**
   * Add or update an attribute of an entity
   * 
   * @param name entity name
   * @param property key of the property
   * @param value value of the property
   * @return Returns {@code true} if the property was set or {@code false} if the entity does not
   *         exist
   */
  boolean setProperty(String name, String property, Object value);

  /**
   * Tells whether an entity exists
   * 
   * @param name entity name
   * @return Returns {@code true} if the entity exists
   */
  boolean isPresent(String name);

  /**
   * Retrieves the value of a property
   * 
   * @param name entity name
   * @param property key of the property
   * @return the value of the requested property or {@code null}
   */
  Object getProperty(String name, String property);
}
