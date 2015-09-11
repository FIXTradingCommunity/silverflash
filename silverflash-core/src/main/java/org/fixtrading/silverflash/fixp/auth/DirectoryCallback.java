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
