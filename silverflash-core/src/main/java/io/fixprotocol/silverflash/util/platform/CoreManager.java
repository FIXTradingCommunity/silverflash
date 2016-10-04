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

package io.fixprotocol.silverflash.util.platform;

import java.util.Arrays;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.PointerType;
import com.sun.jna.ptr.LongByReference;
import com.sun.jna.platform.win32.WinDef;

class CoreManager {

  private interface CLibrary extends Library {
    public static final CLibrary INSTANCE = (CLibrary) Native.loadLibrary(LIBRARY_NAME,
        CLibrary.class);

    int sched_getcpu() throws LastErrorException;

    int sched_setaffinity(final int pid, final int cpusetsize, final PointerType cpuset)
        throws LastErrorException;

    public void SetThreadAffinityMask(final int pid, final WinDef.DWORD lpProcessAffinityMask)
        throws LastErrorException;

    public int GetCurrentThread() throws LastErrorException;

  }

  private static final int[] allCores;
  private static final String LIBRARY_NAME = Platform.isWindows() ? "kernel32" : "c";

  static {
    final int coresCount = Runtime.getRuntime().availableProcessors();
    allCores = new int[coresCount];

    for (int i = 0; i < allCores.length; i++) {
      allCores[i] = i;
    }
  }

  /**
   * Returns all cores
   * 
   * @return array of core numbers, zero-based index
   */
  public static int[] allCores() {
    return allCores;
  }

  /**
   * Returns a subset of cores
   * 
   * @param minCore first core to include
   * @param maxCore last core to include
   * @return array of core numbers, zero-based index
   */
  public static int[] coreRange(int minCore, int maxCore) {
    return Arrays.copyOfRange(allCores, minCore, maxCore + 1);
  }

  /**
   * Returns a subset of cores
   * 
   * @param minCore first core to include
   * @return array of core numbers, zero-based index
   */
  public static int[] coreRange(int minCore) {
    return Arrays.copyOfRange(allCores, minCore, allCores.length);
  }

  public static void setCurrentThreadAffinity(int coreId) {
    if (Platform.isWindows()) {
      setCurrentThreadAffinityWin32(coreId);
    } else {
      setCurrentThreadAffinityMask(coreId);
    }
  }

  private static void setCurrentThreadAffinityWin32(int coreId) {
    final CLibrary lib = CLibrary.INSTANCE;
    try {
      WinDef.DWORD mask = new WinDef.DWORD(1L << coreId);
      lib.SetThreadAffinityMask(lib.GetCurrentThread(), mask);
    } catch (LastErrorException e) {
      System.err.format("Error setting thread affinity; last error: %d", e.getErrorCode());
    }
  }

  private static void setCurrentThreadAffinityMask(int coreId) {
    final CLibrary lib = CLibrary.INSTANCE;
    final long mask = 1L << coreId;
    final int cpuMaskSize = Long.SIZE / 8;
    try {
      final int ret = lib.sched_setaffinity(0, cpuMaskSize, new LongByReference(mask));
      if (ret < 0) {
        throw new Exception("sched_setaffinity( 0, (" + cpuMaskSize + ") , &(" + mask
            + ") ) return " + ret);
      }
    } catch (Throwable e) {
      System.err.format("Error setting thread affinity; %s", e);
    }
  }

}
