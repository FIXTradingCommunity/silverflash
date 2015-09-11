package org.fixtrading.silverflash.util.platform;

/**
 * A Thread that is optionally pinned to a core
 * 
 * @author Don Mendelson
 *
 */
class AffinityThread extends Thread {
  private final int coreToRunOn;
  private final boolean affinityEnabled;

  /**
   * Constructor
   * 
   * @param runnable function for thread to run
   */
  public AffinityThread(Runnable runnable) {
    this(runnable, 0, false, false);
  }

  /**
   * Constructor
   * 
   * @param runnable function for thread to run
   * @param coreToRunOn core to pin this thread to if affinity is enabled
   * @param affinityEnabled set {@code true} to pin to the core
   * @param isDaemon set {@code true} to make a daemon thread
   */
  public AffinityThread(Runnable runnable, int coreToRunOn, boolean affinityEnabled,
      boolean isDaemon) {
    super(runnable);
    this.coreToRunOn = coreToRunOn;
    this.affinityEnabled = affinityEnabled;
    setDaemon(isDaemon);
  }

  @Override
  public void run() {
    if (affinityEnabled) {
      CoreManager.setCurrentThreadAffinity(coreToRunOn);
    }
    super.run();
  }

}
