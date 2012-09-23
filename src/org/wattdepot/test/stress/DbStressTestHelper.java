package org.wattdepot.test.stress;

import static org.wattdepot.server.ServerProperties.DB_IMPL_KEY;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbManager;

/**
 * Helper class for creating test data to be used in the database stress test.
 * 
 * @author George Lee
 * 
 */
public abstract class DbStressTestHelper {
  protected static Server server;
  protected static DbManager manager;

  /**
   * Initialize the server for the stress test. Must be implemented by subclasses.
   * 
   * @throws Exception if there is an error setting the server up.
   */
  public abstract void initializeServer() throws Exception;

  /**
   * Randomly retrieves a row of data from the database. Must be implemented by subclasses.
   * 
   * @throws Exception if there are issues.
   */
  public abstract void testRandomRetrieval() throws Exception;

  /**
   * Randomly retrieves a day's worth of information from WattDepot. Must be implemented by
   * subclasses.
   * 
   * @throws Exception if there are issues.
   */
  public abstract void testRandomDailyIndexes() throws Exception;

  /**
   * 
   */
  /**
   * Helper to generate test sensor data for the stress test.
   * 
   * @param tstamp The timestamp for this sensor data.
   * @param source The source to use.
   * @return Generated SensorData with a made up POWER_CONSUMED value.
   * @throws Exception if the sensor data could not be created.
   */
  protected static SensorData createSensorData(XMLGregorianCalendar tstamp, Source source)
      throws Exception {
    return new SensorData(tstamp, "JUnit", source.toUri(server), new Property(
        SensorData.POWER_CONSUMED, "10000"));
  }

  /**
   * Helper to generate a test user for the stress test.
   * 
   * @param email The email address of the user to create.
   * @return The user with a bogus property and password.
   */
  protected static User makeTestUser(String email) {
    User user = new User(email, "secret", false, null);
    user.addProperty(new Property("awesomeness", "total"));
    return user;
  }

  /**
   * Helper to create a test source for the stress test.
   * 
   * @param name The name of the source.
   * @param user The name of the user that created the source.
   * @param isPublic True if this should be a public source, false otherwise.
   * @param isVirtual True if this should be a virtual source, false otherwise.
   * @return A test source with a bogus carbon intensity property.
   */
  protected static Source createTestSource(String name, User user, boolean isPublic,
      boolean isVirtual) {
    Source source =
        new Source(name, user.toUri(server), isPublic, isVirtual, "21.30078,-157.819129,41",
            "Made up location", "Obvius-brand power meter", null, null);
    source.addProperty(new Property(Source.CARBON_INTENSITY, "294"));
    return source;
  }

}

/**
 * Wrapper class to quickly instantiate multiple clients. Threads need to override run method to execute.
 * 
 * @author George Lee
 *
 */
class ClientThread extends Thread {
  protected WattDepotClient client;
  protected long threadId;

  /**
   * Constructor for the client thread. Instantiates the thread with the server used by the stress test.
   */
  public ClientThread(int id) {
    this.client =
        new WattDepotClient(ParallelStressTest.server.getHostName(), ParallelStressTest.adminEmail,
            ParallelStressTest.adminPassword);
    this.threadId = id;
  }
}

/**
 * Wrapper class to instantiate multiple DbManagers. Threads will need to override run method to execute.
 * 
 * @author George Lee
 *
 */
class ManagerThread extends Thread {
  protected DbManager manager;
  protected long id;

  /**
   * Constructor for the thread.
   * 
   * @param server The server implementation used to set up the thread.
   */
  ManagerThread(Server server, int id) {
    this.manager = new DbManager(server, server.getServerProperties().get(DB_IMPL_KEY), true);
    this.id = id;
  }
}