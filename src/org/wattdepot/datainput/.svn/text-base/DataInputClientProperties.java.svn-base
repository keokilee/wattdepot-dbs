package org.wattdepot.datainput;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Provides access to the values stored in the bmo.properties file. Portions of this code are
 * adapted from http://hackystat-sensorbase-uh.googlecode.com/
 * 
 * @author Philip Johnson
 * @author Robert Brewer
 */
public class DataInputClientProperties {

  /** The WattDepot server URI key. */
  public static final String WATTDEPOT_URI_KEY = "datainput.wattdepot-server.uri";
  /** The WattDepot server username key. */
  public static final String WATTDEPOT_USERNAME_KEY = "datainput.wattdepot-server.username";
  /** The WattDepot server password key. */
  public static final String WATTDEPOT_PASSWORD_KEY = "datainput.wattdepot-server.password";
  /** The data file name key. */
  public static final String FILENAME_KEY = "datainput.filename";
  /** The BuildingManagerOnline URI key. */
  public static final String BMO_URI_KEY = "datainput.bmo.uri";
  /** The BuildingManagerOnline username key. */
  public static final String BMO_USERNAME_KEY = "datainput.bmo.username";
  /** The BuildingManagerOnline password key. */
  public static final String BMO_PASSWORD_KEY = "datainput.bmo.password";
  /** The BuildingManagerOnline DB key. This is a mysterious, opaque parameter from BMO. */
  public static final String BMO_DB_KEY = "datainput.bmo.db";
  /** The BuildingManagerOnline AS key. This is a mysterious, opaque parameter from BMO. */
  public static final String BMO_AS_KEY = "datainput.bmo.as";

  /** Where we store the properties. */
  private Properties properties;

  /**
   * Creates a new ServerProperties instance, initialized from a default properties file. Prints an
   * error to the console if problems occur on loading.
   * 
   * @throws IOException When there is a problem opening the given properties file.
   */
  public DataInputClientProperties() throws IOException {
    this(null);
  }

  /**
   * Creates a new ServerProperties instance using values from the provided properties file. Prints
   * an error to the console if problems occur on loading.
   * 
   * @param propertiesFilename The filename of the properties file to initialize from.
   * @throws IOException When there is a problem opening the given properties file.
   */
  public DataInputClientProperties(String propertiesFilename) throws IOException {
    if (propertiesFilename == null) {
      initializeProperties();
    }
    else {
      initializeProperties(propertiesFilename);
    }
  }

  /**
   * Reads in the properties in ~/.wattdepot/client/datainput.properties, and provides default
   * values for all properties not mentioned in this file.
   * 
   * @throws IOException if errors occur while loading properties file.
   */
  private void initializeProperties() throws IOException {
    String userHome = org.wattdepot.util.logger.WattDepotUserHome.getHomeString();
    String wattDepotHome = userHome + "/.wattdepot";
    String clientHome = wattDepotHome + "/client";
    String propFile = clientHome + "/datainput.properties";
    initializeProperties(propFile);
  }

  /**
   * Reads in the properties the provided file, and provides default values for some properties not
   * mentioned in this file.
   * 
   * @param propertiesFilename The filename of the properties file to initialize from.
   * @throws IOException If there are problems reading from the properties file.
   */
  private void initializeProperties(String propertiesFilename) throws IOException {
    this.properties = new Properties();

    // Set defaults for 'standard' operation.
    // properties.setProperty(FILENAME_KEY, clientHome + "/bmo-data.tsv");
    properties.setProperty(BMO_URI_KEY,
        "http://www.buildingmanageronline.com/members/mbdev_export.php/download.txt");

    FileInputStream stream = null;
    try {
      stream = new FileInputStream(propertiesFilename);
      properties.load(stream);
      System.out.println("Loading data input client properties from: " + propertiesFilename);
    }
    catch (IOException e) {
      System.out.println(propertiesFilename
          + " not found. Using default data input client properties.");
      throw e;
    }
    finally {
      if (stream != null) {
        stream.close();
      }
    }
    trimProperties(properties);
  }

  // /**
  // * Sets the following properties to their "test" equivalents.
  // * <ul>
  // * <li>ADMIN_EMAIL_KEY
  // * <li>ADMIN_PASSWORD_KEY
  // * <li>HOSTNAME_KEY
  // * <li>DB_DIR_KEY
  // * <li>PORT_KEY
  // * </ul>
  // * Also sets TEST_INSTALL_KEY to true.
  // */
  // public void setTestProperties() {
  // properties.setProperty(ADMIN_EMAIL_KEY, properties.getProperty(TEST_ADMIN_EMAIL_KEY));
  // properties.setProperty(ADMIN_PASSWORD_KEY, properties.getProperty(TEST_ADMIN_PASSWORD_KEY));
  // properties.setProperty(HOSTNAME_KEY, properties.getProperty(TEST_HOSTNAME_KEY));
  // properties.setProperty(DB_DIR_KEY, properties.getProperty(TEST_DB_DIR_KEY));
  // properties.setProperty(PORT_KEY, properties.getProperty(TEST_PORT_KEY));
  // properties.setProperty(GVIZ_PORT_KEY, properties.getProperty(TEST_GVIZ_PORT_KEY));
  // properties.setProperty(TEST_INSTALL_KEY, TRUE);
  // // Change the db implementation class if DB_IMPL_KEY is in system properties.
  // String dbImpl = System.getProperty(DB_IMPL_KEY);
  // if (dbImpl != null) {
  // properties.setProperty(DB_IMPL_KEY, dbImpl);
  // }
  // trimProperties(properties);
  // // update the system properties object to reflect these new values.
  // Properties systemProperties = System.getProperties();
  // systemProperties.putAll(properties);
  // System.setProperties(systemProperties);
  // }

  /**
   * Returns a string containing all current properties in alphabetical order.
   * 
   * @return A string with the properties.
   */
  public String echoProperties() {
    String cr = System.getProperty("line.separator");
    String eq = " = ";
    String pad = "                ";
    // Adding them to a treemap has the effect of alphabetizing them.
    TreeMap<String, String> alphaProps = new TreeMap<String, String>();
    for (Map.Entry<Object, Object> entry : this.properties.entrySet()) {
      String propName = (String) entry.getKey();
      String propValue = (String) entry.getValue();
      alphaProps.put(propName, propValue);
    }
    StringBuffer buff = new StringBuffer(25);
    buff.append("client Properties:").append(cr);
    for (String key : alphaProps.keySet()) {
      buff.append(pad).append(key).append(eq).append(get(key)).append(cr);
    }
    return buff.toString();
  }

  /**
   * Returns the value of the Server Property specified by the key.
   * 
   * @param key Should be one of the public static final strings in this class.
   * @return The value of the key, or null if not found.
   */
  public String get(String key) {
    return this.properties.getProperty(key);
  }

  /**
   * Ensures that the there is no leading or trailing whitespace in the property values. The fact
   * that we need to do this indicates a bug in Java's Properties implementation to me.
   * 
   * @param properties The properties.
   */
  private void trimProperties(Properties properties) {
    // Have to do this iteration in a Java 5 compatible manner. no stringPropertyNames().
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String propName = (String) entry.getKey();
      properties.setProperty(propName, properties.getProperty(propName).trim());
    }
  }
}
