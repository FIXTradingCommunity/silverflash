package org.fixtrading.silverflash.auth;

/**
 * Look up entities and their attributes
 * <p>
 * Production systems may use services such as LDAP, but not committing to a particular API here.
 * 
 * @author Don Mendelson
 *
 */
public interface Directory {

  /**
   * Property name for password
   */
  static final String PW_DIRECTORY_ATTR = "PW_DIRECTORY_ATTR";

  /**
   * Add a new entity
   * 
   * @param name entity name
   * @return Returns {@code true} if the entity was added or {@code false} if it already existed
   */
  boolean add(String name);

  /**
   * Add or update an attribute of an entity
   * 
   * @param name entity name
   * @param property key of the property
   * @param value value of the property
   * @return Returns {@code true} if the property was set or {@code false} if the entity does not
   *         exist
   */
  boolean setProperty(String name, String property, Object value);

  /**
   * Tells whether an entity exists
   * 
   * @param name entity name
   * @return Returns {@code true} if the entity exists
   */
  boolean isPresent(String name);

  /**
   * Retrieves the value of a property
   * 
   * @param name entity name
   * @param property key of the property
   * @return the value of the requested property or {@code null}
   */
  Object getProperty(String name, String property);
}
