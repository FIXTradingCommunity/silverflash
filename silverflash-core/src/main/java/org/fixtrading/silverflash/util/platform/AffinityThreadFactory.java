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

package org.fixtrading.silverflash.util.platform;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A ThreadFactory that optionally pins new threads to cores
 * 
 * @author Don Mendelson
 *
 */
public class AffinityThreadFactory implements ThreadFactory {

  private final ThreadFactory nonAffinityThreadFactory = runnable -> AffinityThreadFactory.this.newThread(runnable, false, true);

  private final int[] cores;
  private int coreIndex = 0;
  private final boolean affinityEnabled;
  private final String namePrefix;
  private final AtomicInteger counter = new AtomicInteger();
  private final boolean isDaemon;

  public AffinityThreadFactory(int minCore, boolean affinityEnabled, boolean isDaemon,
      String namePrefix) {
    this(CoreManager.coreRange(minCore), affinityEnabled, isDaemon, namePrefix);
  }

  public AffinityThreadFactory(int minCore, int maxCore, boolean affinityEnabled, boolean isDaemon,
      String namePrefix) {
    this(CoreManager.coreRange(minCore, maxCore), affinityEnabled, isDaemon, namePrefix);
  }

  public AffinityThreadFactory(boolean affinityEnabled, boolean isDaemon, String namePrefix) {
    this(CoreManager.allCores(), affinityEnabled, isDaemon, namePrefix);
  }

  private AffinityThreadFactory(int[] cores, boolean affinityEnabled, boolean isDaemon,
      String namePrefix) {
    this.cores = cores;
    this.affinityEnabled = affinityEnabled;
    this.namePrefix = namePrefix;
    this.isDaemon = isDaemon;
  }

  public Thread newThread(Runnable runnable) {
    return newThread(runnable, this.affinityEnabled, this.isDaemon);
  }

  /**
   * Create a new Thread
   * 
   * @param runnable function to run on this Thread
   * @param affinityEnabled set {@code true} if core affinity is desired
   * @param isDaemon set {@code true} to create daemon Thread
   * @return a new Thread
   */
  public Thread newThread(Runnable runnable, boolean affinityEnabled, boolean isDaemon) {
    AffinityThread affinityThread;

    if (coreIndex < cores.length && affinityEnabled) {
      affinityThread =
          new AffinityThread(runnable, cores[coreIndex % cores.length], affinityEnabled, isDaemon);
      coreIndex++;
    } else {
      affinityThread = new AffinityThread(runnable);
    }

    affinityThread.setName(namePrefix + "-" + counter.incrementAndGet());
    return affinityThread;
  }

  /**
   * Returns a ThreadFactory that uses same naming convention at this factory, but new threads are
   * not pinned to cores.
   * 
   * @return a ThreadFactory for non-affinity thread pools
   */
  public ThreadFactory nonAffinityThreadFactory() {
    return nonAffinityThreadFactory;
  }
}
