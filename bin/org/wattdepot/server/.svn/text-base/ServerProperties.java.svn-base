package org.wattdepot.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Provides access to the values stored in the wattdepot-server.properties file. Portions of this
 * code are adapted from http://hackystat-sensorbase-uh.googlecode.com/
 * 
 * @author Philip Johnson
 * @author Robert Brewer
 */
public class ServerProperties {

  /** The full path to the server's home directory. */
  public static final String SERVER_HOME_DIR = "wattdepot-server.homedir";
  /** The admin email key. */
  public static final String ADMIN_EMAIL_KEY = "wattdepot-server.admin.email";
  /** The admin password. */
  public static final String ADMIN_PASSWORD_KEY = "wattdepot-server.admin.password";
  /** The context root key. */
  public static final String CONTEXT_ROOT_KEY = "wattdepot-server.context.root";
  /** The context root key. */
  public static final String GVIZ_CONTEXT_ROOT_KEY = "wattdepot-server.gviz.context.root";
  /** The database directory key. */
  public static final String DB_DIR_KEY = "wattdepot-server.db.dir";
  /** The database snapshot directory key. */
  public static final String DB_SNAPSHOT_KEY = "wattdepot-server.db.snapshot";
  /** The database implementation class. */
  public static final String DB_IMPL_KEY = "wattdepot-server.db.impl";
  /** The hostname key. */
  public static final String HOSTNAME_KEY = "wattdepot-server.hostname";
  /** The logging level key. */
  public static final String LOGGING_LEVEL_KEY = "wattdepot-server.logging.level";
  /** The Restlet Logging key. */
  public static final String RESTLET_LOGGING_KEY = "wattdepot-server.restlet.logging";
  /** The SMTP host key. */
  public static final String SMTP_HOST_KEY = "wattdepot-server.smtp.host";
  /** The wattdepot-server port key. */
  public static final String PORT_KEY = "wattdepot-server.port";
  /** The wattdepot-server Google Visualization API port key. */
  public static final String GVIZ_PORT_KEY = "wattdepot-server.gviz.port";
  /** The test installation key. */
  public static final String TEST_INSTALL_KEY = "wattdepot-server.test.install";
  /** The test domain key. */
  public static final String TEST_DOMAIN_KEY = "wattdepot-server.test.domain";
  /** The wattdepot-server port key during testing. */
  public static final String TEST_PORT_KEY = "wattdepot-server.test.port";
  /** The wattdepot-server port key during testing. */
  public static final String TEST_GVIZ_PORT_KEY = "wattdepot-server.test.gviz.port";
  /** The wattdepot-server db dir during testing. */
  public static final String TEST_DB_DIR_KEY = "wattdepot-server.test.db.dir";
  /** The database snapshot directory key during testing. */
  public static final String TEST_DB_SNAPSHOT_KEY = "wattdepot-server.test.db.snapshot";
  /** The test admin email key. */
  public static final String TEST_ADMIN_EMAIL_KEY = "wattdepot-server.test.admin.email";
  /** The test admin password. */
  public static final String TEST_ADMIN_PASSWORD_KEY = "wattdepot-server.test.admin.password";
  /** The test hostname. */
  public static final String TEST_HOSTNAME_KEY = "wattdepot-server.test.hostname";
  /** Where we store the properties. */
  private Properties properties;

  private static String FALSE = "false";
  private static String TRUE = "true";

  /**
   * Creates a new ServerProperties instance using the default filename. Prints an error to the
   * console if problems occur on loading.
   */
  public ServerProperties() {
    this(null);
  }

  /**
   * Creates a new ServerProperties instance loaded from the given filename. Prints an error to the
   * console if problems occur on loading.
   * 
   * @param serverSubdir The name of the subdirectory used to store all files for this server.
   */
  public ServerProperties(String serverSubdir) {
    try {
      initializeProperties(serverSubdir);
    }
    catch (Exception e) {
      System.out.println("Error initializing server properties.");
    }
  }

  /**
   * Reads in the properties in ~/.wattdepot/server/wattdepot-server.properties if this file exists,
   * and provides default values for all properties not mentioned in this file. Will also add any
   * pre-existing System properties that start with "wattdepot-server.".
   * 
   * @param serverSubdir The name of the subdirectory used to store all files for this server.
   * @throws Exception if errors occur.
   */
  private void initializeProperties(String serverSubdir) throws Exception {
    String userHome = org.wattdepot.util.logger.WattDepotUserHome.getHomeString();
    String wattDepotHome = userHome + "/.wattdepot/";
    String serverHome;
    if (serverSubdir == null) {
      // Use default directory
      serverHome = wattDepotHome + "server";
    }
    else {
      serverHome = wattDepotHome + serverSubdir;
    }
    String propFile = serverHome + "/wattdepot-server.properties";
    String defaultAdmin = "admin@example.com";
    this.properties = new Properties();
    // Set defaults for 'standard' operation.
    properties.setProperty(SERVER_HOME_DIR, serverHome);
    properties.setProperty(ADMIN_EMAIL_KEY, defaultAdmin);
    properties.setProperty(ADMIN_PASSWORD_KEY, defaultAdmin);
    properties.setProperty(CONTEXT_ROOT_KEY, "wattdepot");
    properties.setProperty(GVIZ_CONTEXT_ROOT_KEY, "gviz");
    properties.setProperty(DB_DIR_KEY, serverHome + "/db");
    properties.setProperty(DB_SNAPSHOT_KEY, serverHome + "/db-snapshot");
    properties.setProperty(DB_IMPL_KEY, "org.wattdepot.server.db.derby.DerbyStorageImplementation");
    properties.setProperty(HOSTNAME_KEY, "localhost");
    properties.setProperty(LOGGING_LEVEL_KEY, "INFO");
    properties.setProperty(RESTLET_LOGGING_KEY, FALSE);
    properties.setProperty(SMTP_HOST_KEY, "mail.hawaii.edu");
    properties.setProperty(PORT_KEY, "8182");
    properties.setProperty(GVIZ_PORT_KEY, "8184");
    properties.setProperty(TEST_DOMAIN_KEY, "example.com");
    properties.setProperty(TEST_INSTALL_KEY, FALSE);
    properties.setProperty(TEST_ADMIN_EMAIL_KEY, defaultAdmin);
    properties.setProperty(TEST_ADMIN_PASSWORD_KEY, defaultAdmin);
    properties.setProperty(TEST_DB_DIR_KEY, serverHome + "/testdb");
    properties.setProperty(TEST_DB_SNAPSHOT_KEY, serverHome + "/testdb-snapshot");
    properties.setProperty(TEST_PORT_KEY, "8183");
    properties.setProperty(TEST_GVIZ_PORT_KEY, "8185");
    properties.setProperty(TEST_HOSTNAME_KEY, "localhost");

    FileInputStream stream = null;
    try {
      stream = new FileInputStream(propFile);
      properties.load(stream);
      System.out.println("Loading Server properties from: " + propFile);
    }
    catch (IOException e) {
      System.out.println(propFile + " not found. Using default server properties.");
    }
    finally {
      if (stream != null) {
        stream.close();
      }
    }
    addServerSystemProperties(this.properties);
    trimProperties(properties);

    // Now add to System properties. Since the Mailer class expects to find this stuff on the
    // System Properties, we will add everything to it. In general, however, clients should not
    // use the System Properties to get at these values, since that precludes running several
    // SensorBases in a single JVM. And just is generally bogus.
    // Properties systemProperties = System.getProperties();
    // systemProperties.putAll(properties);
    // System.setProperties(systemProperties);
  }

  /**
   * Finds any System properties whose key begins with "wattdepot-server.", and adds those key-value
   * pairs to the passed Properties object.
   * 
   * @param properties The properties instance to be updated with the WattDepot system properties.
   */
  private void addServerSystemProperties(Properties properties) {
    Properties systemProperties = System.getProperties();
    for (Map.Entry<Object, Object> entry : systemProperties.entrySet()) {
      String sysPropName = (String) entry.getKey();
      if (sysPropName.startsWith("wattdepot-server.")) {
        String sysPropValue = (String) entry.getValue();
        properties.setProperty(sysPropName, sysPropValue);
      }
    }
  }

  /**
   * Sets the following properties to their "test" equivalents.
   * <ul>
   * <li>ADMIN_EMAIL_KEY
   * <li>ADMIN_PASSWORD_KEY
   * <li>HOSTNAME_KEY
   * <li>DB_DIR_KEY
   * <li>DB_SNAPSHOT_KEY
   * <li>PORT_KEY
   * <li>GVIZ_PORT_KEY
   * </ul>
   * Also sets TEST_INSTALL_KEY to true.
   */
  public void setTestProperties() {
    properties.setProperty(ADMIN_EMAIL_KEY, properties.getProperty(TEST_ADMIN_EMAIL_KEY));
    properties.setProperty(ADMIN_PASSWORD_KEY, properties.getProperty(TEST_ADMIN_PASSWORD_KEY));
    properties.setProperty(HOSTNAME_KEY, properties.getProperty(TEST_HOSTNAME_KEY));
    properties.setProperty(DB_DIR_KEY, properties.getProperty(TEST_DB_DIR_KEY));
    properties.setProperty(DB_SNAPSHOT_KEY, properties.getProperty(TEST_DB_SNAPSHOT_KEY));
    properties.setProperty(PORT_KEY, properties.getProperty(TEST_PORT_KEY));
    properties.setProperty(GVIZ_PORT_KEY, properties.getProperty(TEST_GVIZ_PORT_KEY));
    properties.setProperty(TEST_INSTALL_KEY, TRUE);
    // Change the db implementation class if DB_IMPL_KEY is in system properties.
    String dbImpl = System.getProperty(DB_IMPL_KEY);
    if (dbImpl != null) {
      properties.setProperty(DB_IMPL_KEY, dbImpl);
    }
    trimProperties(properties);
    // update the system properties object to reflect these new values.
    Properties systemProperties = System.getProperties();
    systemProperties.putAll(properties);
    System.setProperties(systemProperties);
  }

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
    buff.append("Server Properties:").append(cr);
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

  /**
   * Returns the fully qualified host name, such as "http://localhost:9876/wattdepot/". Note, the
   * String will end with "/", so there is no need to append another if you are constructing a URI.
   * 
   * @return The fully qualified host name.
   */
  public String getFullHost() {
    return "http://" + get(HOSTNAME_KEY) + ":" + get(PORT_KEY) + "/" + get(CONTEXT_ROOT_KEY) + "/";
  }

  /**
   * Returns the fully qualified host name of the Google Visualization API service, such as
   * "http://localhost:9876/gviz/". Note, the String will end with "/", so there is no need to
   * append another if you are constructing a URI.
   * 
   * @return The fully qualified host name.
   */
  public String getGvizFullHost() {
    return "http://" + get(HOSTNAME_KEY) + ":" + get(GVIZ_PORT_KEY) + "/"
        + get(GVIZ_CONTEXT_ROOT_KEY) + "/";
  }

}
