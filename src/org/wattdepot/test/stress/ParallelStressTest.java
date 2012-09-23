package org.wattdepot.test.stress;

import static org.wattdepot.server.ServerProperties.ADMIN_EMAIL_KEY;
import static org.wattdepot.server.ServerProperties.ADMIN_PASSWORD_KEY;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SubSources;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.test.DataGenerator;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Test the WattDepot server in ways that are designed to make it do hard things (like interpolate
 * and sum up virtual sources). Extended to use parallel threads.
 * 
 * @author Robert Brewer, George Lee
 */
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
public class ParallelStressTest {
  /** Virtual source name. */
  public static final String source11Name = "virtualSource";
  /** Virtual source containing all other sources. */
  public static Source virtualSource;
  /** Array containing all the names of the non-virtual sources. */
  private static final String[] sourceNames = { "source01", "source02", "source03", "source04",
      "source05", "source06", "source07", "source08", "source09", "source10" };
  /** The number of non-virtual sources used for testing. */
  public static final int NUM_SOURCES = sourceNames.length;
  /** The test Sources in URI form. */
  private static final String[] sourceURIs = new String[NUM_SOURCES];
  /** Array containing all the of the non-virtual sources. */
  public static final Source[] sources = new Source[NUM_SOURCES];
  /** The name used for the tool when storing SensorData. */
  private static final String toolName = "DataGenerator";

  private static XMLGregorianCalendar START_TIMESTAMP;
  private static XMLGregorianCalendar END_TIMESTAMP;
  private static final int TEST_CLIENTS = 4;
  /** The WattDepot server used in these tests. */
  protected static Server server = null;
  /** The DbManager used by these tests. */
  protected static DbManager manager;

  /** The admin email. */
  protected static String adminEmail;
  /** The admin password. */
  protected static String adminPassword;

  /**
   * Starts the server going for these tests.
   * 
   * @throws Exception If problems occur setting up the server.
   */
  @BeforeClass
  public static void setupServer() throws Exception {
    ParallelStressTest.server = Server.newTestInstance();

    ParallelStressTest.adminEmail = server.getServerProperties().get(ADMIN_EMAIL_KEY);
    ParallelStressTest.adminPassword = server.getServerProperties().get(ADMIN_PASSWORD_KEY);

    ParallelStressTest.manager = (DbManager) server.getContext().getAttributes().get("DbManager");
    String adminUserUri = manager.getUser(adminEmail).toUri(server);
    System.out.print("Creating test data...");
    ParallelStressTest.START_TIMESTAMP = Tstamp.makeTimestamp("2010-01-08T00:00:00.000-10:00");
    ParallelStressTest.END_TIMESTAMP = Tstamp.makeTimestamp("2010-02-08T00:00:00.000-10:00");

    SubSources subSources = new SubSources();
    for (int i = 0; i < NUM_SOURCES; i++) {
      Source source = new Source(sourceNames[i], adminUserUri, true);
      source.addProperty(new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true"));
      sources[i] = source;
      sourceURIs[i] = source.toUri(server);
      subSources.getHref().add(sourceURIs[i]);
      ParallelStressTest.manager.storeSource(source);
    }
    ParallelStressTest.virtualSource =
        new Source(source11Name, adminUserUri, true, true, "", "", "", null, subSources);
    ParallelStressTest.manager.storeSource(virtualSource);

    Date testStart = new Date();
    parallelLoad(TEST_CLIENTS, START_TIMESTAMP, END_TIMESTAMP, 15);
    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format("Time to insert data: %.1f ms%n", msElapsed);
  }

  /**
   * Load the data in parallel using a provided number of threads.
   * 
   * @param threads The number of threads to use.
   * @param start The start range for the generated data.
   * @param end The end range for the generated data.
   * @param dataInterval The interval in between sensor data.
   */
  public static void parallelLoad(int threads, XMLGregorianCalendar start,
      XMLGregorianCalendar end, final int dataInterval) {
    // Partition time period into separate threads.
    List<ClientThread> clients = new ArrayList<ClientThread>();
    final long begin = start.toGregorianCalendar().getTimeInMillis();
    final long interval = (end.toGregorianCalendar().getTimeInMillis() - begin) / threads;
    final double totalEnergyGenerated[] =
        { 0, 1234.0, 2345, 3456, 4567, 5678, 6789, 7890, 8901, 9012 };

    for (int i = 0; i < threads; i++) {
      clients.add(new ClientThread(i) {
        @Override
        public void run() {
          SensorData data;
          int j = 0;
          Properties props;
          int power = 0;

          XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(begin + (interval * threadId));
          XMLGregorianCalendar myEnd = Tstamp.makeTimestamp(begin + (interval * (threadId + 1)));
          while (Tstamp.lessThan(timestamp, myEnd)) {
            for (int k = 0;k < NUM_SOURCES; k++) {
              props = new Properties();
              power = j * (k + 1) * 100;
              // energy counter increased by power * rate, converted to Wh
              totalEnergyGenerated[k] += power * dataInterval / 60.0;
              props.getProperty().add(
                  new Property(SensorData.POWER_GENERATED, Integer.toString(power)));
              props.getProperty().add(
                  new Property(SensorData.ENERGY_GENERATED_TO_DATE, Double
                      .toString(totalEnergyGenerated[k])));
              data = new SensorData(timestamp, toolName, sourceURIs[k], props);
              try {
                client.storeSensorData(data);
              }
              catch (Exception e) {
                e.printStackTrace();
              }
            }
            timestamp = Tstamp.incrementMilliseconds(timestamp, dataInterval * 1000);
            // Keep ratcheting up j until we reach 100, then reset to 0
            j = (j < 100) ? j + 10 : 0;
          }
        }
      });
    }
  }

  /**
   * Creates new object and creates a client for testing.
   */
  public ParallelStressTest() {
    super();
  }

  /**
   * Computes interpolated power for a single source many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testInterpolatedPowerSingleSource() throws Exception {
    final XMLGregorianCalendar timestamp = Tstamp.makeTimestamp("2010-01-08T12:03:07.000-10:00");
    final int iterations = 1000;
    List<ClientThread> clients = new ArrayList<ClientThread>();
    for (int i = 0; i < TEST_CLIENTS; i++) {
      // Spawn clients
      clients.add(new ClientThread(i) {
        @Override
        public void run() {
          for (int j = 0; j < iterations; j++) {
            try {
              client.getPowerGenerated(DataGenerator.getSourceName(0), timestamp);
            }
            catch (Exception e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
      });
    }
    double msElapsed = this.runClients(clients);

    System.out.format("Time to calculate interpolated power generated %d times: %.1f ms%n", 1000,
        msElapsed);
    System.out.format("Mean time to calculate interpolated power: %.1f ms%n", msElapsed / 1000);
  }

  /**
   * Computes interpolated power for a virtual source many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  public void testInterpolatedPowerVirtualSource() throws Exception {
    final XMLGregorianCalendar timestamp = Tstamp.makeTimestamp("2010-01-08T12:03:07.000-10:00");
    final int iterations = 1000;
    List<ClientThread> clients = new ArrayList<ClientThread>();
    for (int i = 0; i < TEST_CLIENTS; i++) {
      clients.add(new ClientThread(i) {
        @Override
        public void run() {
          for (int j = 0; j < iterations; j++) {
            try {
              client.getPowerGenerated(DataGenerator.source11Name, timestamp);
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }
    double msElapsed = this.runClients(clients);
    System.out.format(
        "Time to calculate interpolated power for virtual source generated %d times: %.1f ms%n",
        iterations, msElapsed);
    System.out.format("Mean time to calculate interpolated power for virtual source: %.1f ms%n",
        msElapsed / iterations);
  }

  /**
   * Computes calculated energy for a virtual source many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testEnergyVirtualSource() throws Exception {
    final XMLGregorianCalendar startTime = Tstamp.incrementMinutes(START_TIMESTAMP, 1);
    final XMLGregorianCalendar endTime = Tstamp.incrementMinutes(END_TIMESTAMP, -30);
    final int iterations = 100;

    List<ClientThread> clients = new ArrayList<ClientThread>();
    for (int i = 0; i < TEST_CLIENTS; i++) {
      // Initialize clients
      clients.add(new ClientThread(i) {
        @Override
        public void run() {
          for (int j = 0; j < iterations; j++) {
            try {
              client.getEnergyGenerated(DataGenerator.source11Name, startTime, endTime, 0);
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }

    double msElapsed = this.runClients(clients);
    System.out.format(
        "Time to compute energy for virtual source using counters generated %d times: %.1f ms%n",
        iterations, msElapsed);
    System.out.format("Mean time to compute energy for virtual source using counters: %.1f ms%n",
        msElapsed / iterations);
  }

  /**
   * Retrieves latest sensor data from sources many times and reports how long it took.
   * 
   * @throws Exception If there are problems.
   */
  @Test
  public void testLatestSensorData() throws Exception {
    final int iterations = 10; // per source
    List<ClientThread> clients = new ArrayList<ClientThread>();
    for (int i = 0; i < TEST_CLIENTS; i++) {
      // Initialize clients
      clients.add(new ClientThread(i) {
        @Override
        public void run() {
          for (int j = 0; j < iterations; j++) {
            try {
              for (int k = 0; k < DataGenerator.NUM_SOURCES; k++) {
                client.getLatestSensorData(DataGenerator.getSourceName(k));
              }
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }
    double msElapsed = this.runClients(clients);
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
    final int iterations = 100; // per source
    List<ClientThread> clients = new ArrayList<ClientThread>();
    for (int i = 0; i < TEST_CLIENTS; i++) {
      // Initialize clients
      clients.add(new ClientThread(i) {
        @Override
        public void run() {
          for (int j = 0; j < iterations; j++) {
            try {
              client.getLatestSensorData(DataGenerator.source11Name);
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }
    double msElapsed = this.runClients(clients);
    System.out.format(
        "Time to retrieve latest sensor data from virtual source %d times: %.1f ms%n", iterations,
        msElapsed);
    System.out.format("Mean time to retrieve latest sensor data from virtual source : %.1f ms%n",
        msElapsed / iterations);
  }

  /**
   * Runs the client threads in the parallel test.
   * 
   * @param clients The threads to run.
   * @return The time taken to run the threads.
   * @throws InterruptedException if a thread is interrupted.
   */
  private double runClients(List<ClientThread> clients) throws InterruptedException {
    Date testStart = new Date();
    // Execute threads.
    for (ClientThread client : clients) {
      client.start();
    }

    // Block until clients are done.
    boolean isComplete = false;
    while (!isComplete) {
      isComplete = true;
      for (ClientThread client : clients) {
        isComplete = isComplete && !client.isAlive();
      }
      // Prevent busy wait.
      if (!isComplete) {
        Thread.sleep(500);
      }
    }

    Date testEnd = new Date();
    return testEnd.getTime() - testStart.getTime();
  }
}
