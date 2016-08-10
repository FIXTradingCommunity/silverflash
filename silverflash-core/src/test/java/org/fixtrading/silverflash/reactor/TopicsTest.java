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

package org.fixtrading.silverflash.reactor;

import static org.junit.Assert.*;

import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.Topics;
import org.junit.Test;

public class TopicsTest {


  @Test
  public void testGetTopicString() {
    Topic joe = Topics.getTopic("Joe");
    Topic bob = Topics.getTopic("Bob");

    assertTrue(!joe.equals(bob));
    assertTrue(joe.hashCode() != bob.hashCode());
    assertTrue(joe.compareTo(bob) > 0);
    assertTrue(bob.compareTo(joe) < 0);

    assertTrue(joe.equals(joe));
    assertEquals(0, joe.compareTo(joe));
  }

  @Test
  public void testGetTopicStringArray() {
    Topic joe = Topics.getTopic("Joe");
    Topic joebob = Topics.getTopic("Joe", "Bob");
    Topic bobjoe = Topics.getTopic("Bob", "Joe");

    assertTrue(!joebob.equals(bobjoe));
    assertTrue(joebob.compareTo(bobjoe) > 0);
    assertTrue(bobjoe.compareTo(joebob) < 0);

    assertTrue(!joe.equals(joebob));
    assertTrue(joe.compareTo(joebob) < 0);
    assertTrue(joebob.compareTo(joe) > 0);
  }

  @Test
  public void testGetSubtopic() {
    Topic joe = Topics.getTopic("Joe");
    Topic joebob1 = Topics.getTopic("Joe", "Bob");
    Topic joebob2 = Topics.getSubtopic(joe, "Bob");
    assertTrue(joe.isSubtopic(joebob2));
    assertTrue(joebob1.equals(joebob2));
    assertEquals(0, joebob1.compareTo(joebob2));
    assertEquals(0, joebob2.compareTo(joebob1));
    assertEquals(joebob1.hashCode(), joebob2.hashCode());
  }
}
