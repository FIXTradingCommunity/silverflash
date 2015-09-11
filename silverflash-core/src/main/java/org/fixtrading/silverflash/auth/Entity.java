package org.fixtrading.silverflash.auth;

import java.security.Principal;

/**
 * Identifier of a business entity that is responsible for transactions on a Session
 * 
 * @author Don Mendelson
 *
 */
public class Entity implements Principal {

  private final String name;

  public Entity(String name) {
    this.name = name;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.security.Principal#getName()
   */
  public String getName() {
    return name;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Entity [");
    if (name != null) {
      builder.append("name=");
      builder.append(name);
    }
    builder.append("]");
    return builder.toString();
  }

}
