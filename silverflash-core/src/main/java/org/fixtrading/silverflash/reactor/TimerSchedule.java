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

import java.util.TimerTask;

/**
 * Wraps a TimerTask
 * 
 * @author Don Mendelson
 *
 */
public class TimerSchedule {

  private final TimerTask task;

  TimerSchedule(TimerTask task) {
    this.task = task;
  }

  /**
   * 
   * @return true if this task is scheduled for one-time execution and has not yet run, or this task
   *         is scheduled for repeated execution. Returns false if the task was scheduled for
   *         one-time execution and has already run, or if the task was never scheduled, or if the
   *         task was already cancelled. (Loosely speaking, this method returns <tt>true</tt> if it
   *         prevents one or more scheduled executions from taking place.)
   */
  public boolean cancel() {
    return task.cancel();
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    cancel();
  }
}
