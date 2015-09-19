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

/**
 * Represents the lifetime of a during which a reactive will receive events on a Topic
 * <p>
 * The reactive event holds a Subscription as token. It may be used to unsubscribe. Also, if the a
 * Receiver and its Subscription goes out of scope, it unsubscribes automatically to prevent memory
 * leaks.
 * 
 * @author Don Mendelson
 *
 */
public class Subscription {

  private final EventReactor<?> reactor;
  private final Topic topic;

  Subscription(Topic topic, EventReactor<?> reactor) {
    this.topic = topic;
    this.reactor = reactor;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Subscription other = (Subscription) obj;
    if (topic == null) {
      if (other.topic != null)
        return false;
    } else if (!topic.equals(other.topic))
      return false;
    return true;
  }

  /**
   * @return the topic
   */
  public Topic getTopic() {
    return topic;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((topic == null) ? 0 : topic.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "Subscription [topic=" + topic + "]";
  }

  /**
   * Unsubscribe for events on the Topic
   */
  public void unsubscribe() {
    reactor.unsubscribe(topic);
  }

  /**
   * Unsubscribes if this Subscription goes out of scope
   */
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    unsubscribe();
  }
}
