package org.wattdepot.test;

import static org.junit.Assert.assertTrue;
import static org.wattdepot.server.ServerProperties.ADMIN_EMAIL_KEY;
import static org.wattdepot.server.ServerProperties.ADMIN_PASSWORD_KEY;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.Before;
import org.junit.BeforeClass;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SubSources;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.server.Server;
import org.wattdepot.server.ServerProperties;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Provides helpful utility methods to WattDepot resource test classes, which will normally want to
 * extend this class. Portions of this code are adapted from
 * http://hackystat-sensorbase-uh.googlecode.com/
 * 
 * @author Robert Brewer
 * @author Philip Johnson
 */

public class ServerTestHelper {
  /** The WattDepot server used in these tests. */
  protected static Server server = null;
  /** The DbManager used by these tests. */
  protected static DbManager manager;

  /** The admin email. */
  protected static String adminEmail;
  /** The admin password. */
  protected static String adminPassword;

  /** Name of the default public source. */
  public static final String defaultPublicSource = "saunders-hall";
  /** Name of the default private source. */
  public static final String defaultPrivateSource = "secret-place";
  /** Name of the default virtual source. */
  public static final String defaultVirtualSource = "virtual-source";
  /** Username of the default user that owns both default sources. */
  public static final String defaultOwnerUsername = "joebogus@example.com";
  /** Password of the default user that owns both default sources. */
  public static final String defaultOwnerPassword = "totally-bogus";
  /** Username of the default user that owns no sources. */
  public static final String defaultNonOwnerUsername = "jimbogus@example.com";
  /** Password of the default user that owns no sources. */
  public static final String defaultNonOwnerPassword = "super-bogus";

  protected static XMLGregorianCalendar timestamp1, timestamp2, timestamp3;

  // public ServerTestHelper () throws Exception {
  // ServerTestHelper.server = Server.newTestInstance();
  //
  // ServerTestHelper.adminEmail = server.getServerProperties().get(ADMIN_EMAIL_KEY);
  // ServerTestHelper.adminPassword = server.getServerProperties().get(ADMIN_PASSWORD_KEY);
  //    
  // ServerTestHelper.manager = (DbManager) server.getContext().getAttributes().get("DbManager");
  // }

  /**
   * Starts the server going for these tests.
   * 
   * @throws Exception If problems occur setting up the server.
   */
  @BeforeClass
  public static void setupServer() throws Exception {
    ServerTestHelper.server = Server.newTestInstance();

    ServerTestHelper.adminEmail = server.getServerProperties().get(ADMIN_EMAIL_KEY);
    ServerTestHelper.adminPassword = server.getServerProperties().get(ADMIN_PASSWORD_KEY);

    ServerTestHelper.manager = (DbManager) server.getContext().getAttributes().get("DbManager");
    timestamp1 = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    timestamp2 = Tstamp.makeTimestamp("2009-07-28T09:15:00.000-10:00");
    timestamp3 = Tstamp.makeTimestamp("2009-07-28T09:30:00.000-10:00");
  }

  /**
   * Sets up the DB before each test by wiping the DB and adding the default data.
   */
  @Before
  public void setupDB() {
    // Need to create default data fresh for each test
    manager.wipeData();
    assertTrue("Unable to create default data", createDefaultData());
  }

  /**
   * Kludges up some default data so that SensorData can be stored. Originally this was to support a
   * demo (since there was no way to create sources or users), but now some tests use this data, so
   * it has been moved here.
   * 
   * @return True if the default data could be created, or false otherwise.
   */
  public boolean createDefaultData() {
    // Need to (re)create admin user, since the database gets wiped by each test
    ServerProperties serverProps =
        (ServerProperties) server.getContext().getAttributes().get("ServerProperties");
    String adminUsername = serverProps.get(ServerProperties.ADMIN_EMAIL_KEY);
    String adminPassword = serverProps.get(ServerProperties.ADMIN_PASSWORD_KEY);
    // create the admin User object based on the server properties
    User adminUser = new User(adminUsername, adminPassword, true, null);
    // stick admin user into database
    if (!manager.storeUser(adminUser)) {
      // server.getLogger().severe("Unable to create admin user from properties!");
      return false;
    }
    // create a non-admin user that owns a source for testing
    User ownerUser = new User(defaultOwnerUsername, defaultOwnerPassword, false, null);
    if (!manager.storeUser(ownerUser)) {
      return false;
    }
    // create a non-admin user that owns nothing for testing
    User nonOwnerUser = new User(defaultNonOwnerUsername, defaultNonOwnerPassword, false, null);
    if (!manager.storeUser(nonOwnerUser)) {
      return false;
    }

    // create public source
    Source source1 =
        new Source(defaultPublicSource, ownerUser.toUri(server), true, false,
            "21.30078,-157.819129,41", "Saunders Hall on the University of Hawaii at Manoa campus",
            "Obvius-brand power meter", null, null);
    source1.addProperty(new Property(Source.CARBON_INTENSITY, "1000"));
    // stick public source into database
    if (!manager.storeSource(source1)) {
      return false;
    }

    Source source2 =
        new Source(defaultPrivateSource, ownerUser.toUri(server), false, false,
            "21.35078,-157.819129,41", "Made up private place", "Foo-brand power meter", null, null);
    source2.addProperty(new Property(Source.CARBON_INTENSITY, "3000"));
    // stick public source into database
    if (!manager.storeSource(source2)) {
      return false;
    }

    SubSources subSources = new SubSources();
    subSources.getHref().add(source1.toUri(server));
    subSources.getHref().add(source2.toUri(server));

    Source virtualSource =
        new Source(defaultVirtualSource, ownerUser.toUri(server), true, true,
            "31.30078,-157.819129,41", "Made up location 3", "Virtual source", null, subSources);
    return (manager.storeSource(virtualSource));
  }

  /**
   * Returns the hostname associated with this test server.
   * 
   * @return The host name, including the context root and ending in "/".
   */
  protected static String getHostName() {
    return ServerTestHelper.server.getHostName();
  }

  /**
   * Creates a SensorData for use in testing, 1 in a series.
   * 
   * @return The freshly created SensorData object.
   * @throws Exception If there are problems converting timestamp string to XMLGregorianCalendar
   * (should never happen)
   */
  protected SensorData makeTestSensorData1() throws Exception {
    return new SensorData(timestamp1, "JUnit", Source.sourceToUri(defaultPublicSource, server),
        new Property(SensorData.POWER_CONSUMED, "10000"));
  }

  /**
   * Creates a SensorData for use in testing, 2 in a series.
   * 
   * @return The freshly created SensorData object.
   * @throws Exception If there are problems converting timestamp string to XMLGregorianCalendar
   * (should never happen)
   */
  protected SensorData makeTestSensorData2() throws Exception {
    return new SensorData(timestamp2, "FooTool", Source.sourceToUri(defaultPublicSource, server),
        new Property(SensorData.POWER_CONSUMED, "11000"));
  }

  /**
   * Creates a SensorData for use in testing, 3 in a series.
   * 
   * @return The freshly created SensorData object.
   * @throws Exception If there are problems converting timestamp string to XMLGregorianCalendar
   * (should never happen)
   */
  protected SensorData makeTestSensorData3() throws Exception {
    return new SensorData(timestamp3, "JUnit", Source.sourceToUri(defaultPublicSource, server),
        new Property(SensorData.POWER_CONSUMED, "9500"));
  }

  /**
   * Creates a SensorData for use in testing, for the default private Source.
   * 
   * @return The freshly created SensorData object.
   * @throws Exception If there are problems converting timestamp string to XMLGregorianCalendar
   * (should never happen)
   */
  protected SensorData makeTestSensorDataPrivateSource() throws Exception {
    return new SensorData(Tstamp.makeTimestamp("2009-07-28T09:40:00.000-10:00"), "JUnit", Source
        .sourceToUri(defaultPrivateSource, server), new Property(SensorData.POWER_CONSUMED, "3000"));
  }
}