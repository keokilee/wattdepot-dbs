package org.wattdepot.util.logger;

import java.io.File;

/**
 * Provides a utility that returns the desired parent directory where the ".wattdepot" directory
 * will be created for storage of WattDepot preferences, logs, database, etc. This defaults to the
 * user.home System Property, but can be overridden by the user by providing the property
 * wattdepot.user.home. Portions of this code are adapted from
 * http://hackystat-utilities.googlecode.com/
 * 
 * @author Philip Johnson
 * @author Robert Brewer
 */
public final class WattDepotUserHome {

  /** Name of property used to store the preferred location for user home directory. */
  public static final String WATTDEPOT_USER_HOME_PROPERTY = "wattdepot.user.home";

  /** Make this class non-instantiable. */
  private WattDepotUserHome() {
    // Do nothing.
  }

  /**
   * Return a File instance representing the desired location of the .wattdepot directory. Note that
   * this directory may or may not exist.
   * 
   * @return A File instance representing the desired user.home directory.
   */
  public static File getHome() {
    return new File(getHomeString());
  }

  /**
   * Return a String representing the desired location of the .wattdepot directory. Note that this
   * directory may or may not exist.
   * 
   * @return A File instance representing the desired user.home directory.
   */
  public static String getHomeString() {
    return System.getProperty(WATTDEPOT_USER_HOME_PROPERTY, System.getProperty("user.home"));
  }
}
