package org.wattdepot.server.db;

import static org.wattdepot.server.ServerProperties.DB_IMPL_KEY;
import org.junit.Before;
import org.junit.BeforeClass;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SubSources;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.server.Server;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Provides helpful utility methods to DbManager test classes, which will normally want to extend
 * this class.
 * 
 * @author Robert Brewer
 */
public class DbManagerTestHelper {

  /** The DbManager under test. */
  protected DbManager manager;

  /** The server being used for these tests. */
  protected static Server server;

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

  /**
   * Creates a test server to use for this set of tests. The DbManager is created separately.
   * 
   * @throws Exception If a problem is encountered.
   */
  @BeforeClass
  public static void startServer() throws Exception {
    DbManagerTestHelper.server = Server.newTestInstance();
  }

  /**
   * Creates a fresh DbManager for each test. This might prove too expensive for some
   * DbImplementations, but we'll cross that bridge when we get there.
   */
  @Before
  public void makeDb() {
    // Will test whatever DbImplementation has been specified in ServerProperties. Note that this
    // can be changed using system Properties, which is useful for running JUnit tests with a
    // particular implementation specified.
    this.manager = new DbManager(server, server.getServerProperties().get(DB_IMPL_KEY), true);
    // Need to create default data for each fresh DbManager
//    assertTrue("Unable to create default data", createDefaultData());
  }

//  /**
//   * Kludges up some default data so that SensorData can be stored. This is a total hack, and should
//   * be removed as soon as the all the resources have been fully implemented.
//   * 
//   * @return True if the default data could be created, or false otherwise.
//   */
//  public boolean createDefaultData() {
//   // Always want there to be an admin user
//   ServerProperties serverProps =
//       (ServerProperties) server.getContext().getAttributes().get("ServerProperties");
//   String adminUsername = serverProps.get(ServerProperties.ADMIN_EMAIL_KEY);
//   String adminPassword = serverProps.get(ServerProperties.ADMIN_PASSWORD_KEY);
//   // create the admin User object based on the server properties
//   User adminUser = new User(adminUsername, adminPassword, true, null);
//   // stick admin user into database
//   if (!this.manager.storeUser(adminUser)) {
//     // server.getLogger().severe("Unable to create admin user from properties!");
//     return false;
//   }
//   // create a non-admin user that owns a source for testing
//   User ownerUser = new User(defaultOwnerUsername, defaultOwnerPassword, false, null);
//   if (!this.manager.storeUser(ownerUser)) {
//     return false;
//   }
//   // create a non-admin user that owns nothing for testing
//   User nonOwnerUser = new User(defaultNonOwnerUsername, defaultNonOwnerPassword, false, null);
//   if (!this.manager.storeUser(nonOwnerUser)) {
//     return false;
//   }
//
//   // create public source
//   Source source1 =
//       new Source(defaultPublicSource, ownerUser.toUri(server), true, false,
//           "21.30078,-157.819129,41", "Saunders Hall on the University of Hawaii at Manoa campus",
//           "Obvius-brand power meter", null, null);
//   source1.addProperty(new Property(Source.CARBON_INTENSITY, "1000"));
//   // stick public source into database
//   if (!this.manager.storeSource(source1)) {
//     return false;
//   }
//
//   Source source2 =
//       new Source(defaultPrivateSource, ownerUser.toUri(server), false, false,
//           "21.35078,-157.819129,41", "Made up private place", "Foo-brand power meter", null, null);
//   source2.addProperty(new Property(Source.CARBON_INTENSITY, "3000"));
//   // stick public source into database
//   if (!this.manager.storeSource(source2)) {
//     return false;
//   }
//
//   SubSources subSources = new SubSources();
//   subSources.getHref().add(source1.toUri(server));
//   subSources.getHref().add(source2.toUri(server));
//
//   Source virtualSource =
//       new Source(defaultVirtualSource, ownerUser.toUri(server), true, true,
//           "31.30078,-157.819129,41", "Made up location 3", "Virtual source", null, subSources);
//   return (this.manager.storeSource(virtualSource));
// }

  /**
   * Creates a user for use in testing, 1 in a series.
   * 
   * @return The freshly created User object.
   */
  protected User makeTestUser1() {
    User user = new User("foo@example.com", "secret", false, null);
    user.addProperty(new Property("aweseomness", "total"));
    return user;
  }

  /**
   * Creates a user for use in testing, 2 in a series.
   * 
   * @return The freshly created User object.
   */
  protected User makeTestUser2() {
    return new User("bar@example.com", "hidden", true, null);
  }

  /**
   * Creates a user for use in testing, 3 in a series.
   * 
   * @return The freshly created User object.
   */
  protected User makeTestUser3() {
    return new User("baz@example.com", "extra-secret", false, null);
  }

  /**
   * Creates a Source for use in testing, 1 in a series.
   * 
   * @return The freshly created Source object.
   */
  protected Source makeTestSource1() {
    Source source =
        new Source("hale-foo", makeTestUser1().toUri(server), true, false,
            "21.30078,-157.819129,41", "Made up location", "Obvius-brand power meter", null, null);
    source.addProperty(new Property(Source.CARBON_INTENSITY, "294"));
    return source;
  }

  /**
   * Creates a Source for use in testing, 2 in a series.
   * 
   * @return The freshly created Source object.
   */
  protected Source makeTestSource2() {
    Source source =
        new Source("hale-bar", makeTestUser2().toUri(server), false, false,
            "31.30078,-157.819129,41", "Made up location 2", "Bogus-brand power meter", null, null);
    source.addProperty(new Property(Source.CARBON_INTENSITY, "128"));
    return source;
  }

  /**
   * Creates a virtual Source for use in testing, 3 in a series.
   * 
   * @return The freshly created Source object.
   */
  protected Source makeTestSource3() {
    SubSources subSources = new SubSources();
    subSources.getHref().add(makeTestSource1().toUri(server));
    subSources.getHref().add(makeTestSource2().toUri(server));
    return new Source("virtual-hales", makeTestUser3().toUri(server), false, true,
        "31.30078,-157.819129,41", "Made up location 3", "Virtual source", null, subSources);
  }

  /**
   * Creates a SensorData for use in testing, 1 in a series.
   * 
   * @return The freshly created SensorData object.
   * @throws Exception If there are problems converting timestamp string to XMLGregorianCalendar
   * (should never happen)
   */
  protected SensorData makeTestSensorData1() throws Exception {
    return new SensorData(Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00"), "JUnit",
        makeTestSource1().toUri(server), new Property(SensorData.POWER_CONSUMED, "10000"));
  }

  /**
   * Creates a SensorData for use in testing, 2 in a series.
   * 
   * @return The freshly created SensorData object.
   * @throws Exception If there are problems converting timestamp string to XMLGregorianCalendar
   * (should never happen)
   */
  protected SensorData makeTestSensorData2() throws Exception {
    return new SensorData(Tstamp.makeTimestamp("2009-07-28T09:15:00.000-10:00"), "FooTool",
        makeTestSource1().toUri(server), new Property(SensorData.POWER_CONSUMED, "11000"));
  }

  /**
   * Creates a SensorData for use in testing, 3 in a series.
   * 
   * @return The freshly created SensorData object.
   * @throws Exception If there are problems converting timestamp string to XMLGregorianCalendar
   * (should never happen)
   */
  protected SensorData makeTestSensorData3() throws Exception {
    return new SensorData(Tstamp.makeTimestamp("2009-07-28T09:30:00.000-10:00"), "JUnit",
        makeTestSource1().toUri(server), new Property(SensorData.POWER_CONSUMED, "9500"));
  }
}
