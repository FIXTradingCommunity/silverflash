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
