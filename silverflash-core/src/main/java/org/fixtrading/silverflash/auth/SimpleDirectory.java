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

package org.fixtrading.silverflash.auth;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple local Directory
 * <p>
 * Implementation is thread-safe and serializable.
 * 
 * @author Don Mendelson
 *
 */
public class SimpleDirectory implements Directory, Serializable {

  private static final long serialVersionUID = 3210975686696739965L;
  private final Map<String, Map<String, Object>> entries = new ConcurrentHashMap<>();

  public boolean add(String name) {
    return entries.putIfAbsent(name, Collections.synchronizedMap(new HashMap<>())) == null;
  }

  public boolean setProperty(String name, String property, Object value) {
    Map<String, Object> props = entries.get(name);
    if (props != null) {
      synchronized (props) {
        props.put(property, value);
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean isPresent(String name) {
    return entries.containsKey(name);
  }

  public Object getProperty(String name, String property) {
    Map<String, Object> props = entries.get(name);
    if (props != null) {
      synchronized (props) {
        return props.get(property);
      }
    } else {
      return null;
    }
  }

}
