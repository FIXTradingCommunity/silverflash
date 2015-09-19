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

package org.fixtrading.silverflash.fixp.auth;

import javax.security.auth.callback.Callback;

import org.fixtrading.silverflash.auth.Directory;

/**
 * @author Don Mendelson
 *
 */
class DirectoryCallback implements Callback {

  private Directory directory;

  /**
   * @param directory
   */
  void setDirectory(Directory directory) {
    this.directory = directory;
  }

  /**
   * @return the directory
   */
  Directory getDirectory() {
    return directory;
  }

}
