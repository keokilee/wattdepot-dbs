package org.wattdepot.test.stress;

import static org.wattdepot.server.ServerProperties.ADMIN_EMAIL_KEY;
import static org.wattdepot.server.ServerProperties.ADMIN_PASSWORD_KEY;
import java.util.Date;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.energy.TestEnergyResource;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.test.DataGenerator;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Test the WattDepot server in ways that are designed to make it do hard things (like interpolate
 * and sum up virtual sources).
 * 
 * @author Robert Brewer
 */
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
public class StressTest {

  private static XMLGregorianCalendar START_TIMESTAMP;
  private static XMLGregorianCalendar END_TIMESTAMP;
  /** The WattDepot server used in these tests. */
  protected static Server server = null;
  /** The DbManager used by these tests. */
  protected static DbManager manager;

  /** The admin email. */
  protected static String adminEmail;
  /** The admin password. */
  protected static String adminPassword;

  private WattDepotClient client;

  /**
   * Starts the server going for these tests.
   * 
   * @throws Exception If problems occur setting up the server.
   */
  @BeforeClass
  public static void setupServer() throws Exception {
    StressTest.server = Server.newTestInstance();

    StressTest.adminEmail = server.getServerProperties().get(ADMIN_EMAIL_KEY);
    StressTest.adminPassword = server.getServerProperties().get(ADMIN_PASSWORD_KEY);

    StressTest.manager = (DbManager) server.getContext().getAttributes().get("DbManager");
    String adminUserUri = manager.getUser(adminEmail).toUri(server);
    DataGenerator test = new DataGenerator(manager, adminUserUri, server);
    System.out.print("Creating test data...");
    StressTest.START_TIMESTAMP = Tstamp.makeTimestamp("2010-01-08T00:00:00.000-10:00");
    StressTest.END_TIMESTAMP = Tstamp.makeTimestamp("2010-01-09T00:00:00.000-10:00");

    test.storeData(START_TIMESTAMP, END_TIMESTAMP, 5);
    System.out.println("done");
  }

  /**
   * Creates new object and creates a client for testing.
   */
  public StressTest() {
    super();
    this.client =
        new WattDepotClient(StressTest.server.getHostName(),
            manager.getUser(adminEmail).getEmail(), manager.getUser(adminEmail).getPassword());
  }

  /**
   * Computes interpolated power for a single source many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testInterpolatedPowerSingleSource() throws Exception {
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp("2010-01-08T12:03:07.000-10:00");
    Date testStart = new Date();
    int iterations = 1000;
    for (int i = 0; i < iterations; i++) {
      client.getPowerGenerated(DataGenerator.getSourceName(0), timestamp);
    }
    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format("Time to calculate interpolated power generated %d times: %.1f ms%n",
        iterations, msElapsed);
    System.out.format("Mean time to calculate interpolated power: %.1f ms%n", msElapsed
        / iterations);
  }

  /**
   * Computes interpolated power for a virtual source many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testInterpolatedPowerVirtualSource() throws Exception {
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp("2010-01-08T12:03:07.000-10:00");
    Date testStart = new Date();
    int iterations = 100;
    for (int i = 0; i < iterations; i++) {
      client.getPowerGenerated(DataGenerator.source11Name, timestamp);
    }
    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format(
        "Time to calculate interpolated power for virtual source generated %d times: %.1f ms%n",
        iterations, msElapsed);
    System.out.format("Mean time to calculate interpolated power for virtual source: %.1f ms%n",
        msElapsed / iterations);
  }

  /**
   * Computes interpolated power for a virtual source many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testSourceSummaryVirtualSource() throws Exception {
    Date testStart = new Date();
    int iterations = 1;
    for (int i = 0; i < iterations; i++) {
      client.getSourceSummary(DataGenerator.source11Name);
    }
    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format(
        "Time to generate source summary for virtual source generated %d times: %.1f ms%n",
        iterations, msElapsed);
    System.out.format("Mean time to generate source summary for virtual source: %.1f ms%n",
        msElapsed / iterations);
  }

  /**
   * Computes calculated energy for a virtual source many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testEnergyVirtualSource() throws Exception {
    Date testStart = new Date();
    XMLGregorianCalendar startTime = Tstamp.incrementMinutes(START_TIMESTAMP, 1), endTime =
        Tstamp.incrementMinutes(END_TIMESTAMP, -10);
    int iterations = 100;
    // First compute using energy counters (default for source)
    for (int i = 0; i < iterations; i++) {
      // 
      client.getEnergyGenerated(DataGenerator.source11Name, startTime, endTime, 0);
    }
    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format(
        "Time to compute energy for virtual source using counters generated %d times: %.1f ms%n",
        iterations, msElapsed);
    System.out.format("Mean time to compute energy for virtual source using counters: %.1f ms%n",
        msElapsed / iterations);

    // Now compute without counters
    for (int i = 0; i < DataGenerator.NUM_SOURCES; i++) {
      TestEnergyResource.removeEnergyCounterProperty(client, DataGenerator.getSourceName(i));
    }
    testStart = new Date();
    for (int i = 0; i < iterations; i++) {
      // 
      client.getEnergyGenerated(DataGenerator.source11Name, startTime, endTime, 0);
    }
    testEnd = new Date();
    msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format(
        "Time to compute energy for virtual source without counters generated %d times: %.1f ms%n",
        iterations, msElapsed);
    System.out.format("Mean time to compute energy for virtual source without counters: %.1f ms%n",
        msElapsed / iterations);
    // Put source properties back in case there are other tests that follow this one
    for (int i = 0; i < DataGenerator.NUM_SOURCES; i++) {
      TestEnergyResource.addEnergyCounterProperty(client, DataGenerator.getSourceName(i));
    }
  }

  /**
   * Retrieves latest sensor data from sources many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testLatestSensorData() throws Exception {
    int iterations = 10; // per source
    Date testStart = new Date();
    for (int i = 0; i < iterations; i++) {
      for (int j = 0; j < DataGenerator.NUM_SOURCES; j++) {
        client.getLatestSensorData(DataGenerator.getSourceName(j));
      }
    }
    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    int calls = iterations * DataGenerator.NUM_SOURCES;
    System.out.format("Time to retrieve latest sensor data %d times: %.1f ms%n", calls, msElapsed);
    System.out.format("Mean time to retrieve latest sensor data: %.1f ms%n", msElapsed / calls);
  }

  /**
   * Retrieves latest sensor data from sources many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testLatestSensorDataVirtual() throws Exception {
    int iterations = 100; // per source
    Date testStart = new Date();
    for (int i = 0; i < iterations; i++) {
      client.getLatestSensorData(DataGenerator.source11Name);
    }
    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format(
        "Time to retrieve latest sensor data from virtual source %d times: %.1f ms%n", iterations,
        msElapsed);
    System.out.format("Mean time to retrieve latest sensor data from virtual source : %.1f ms%n",
        msElapsed / iterations);
  }
}
