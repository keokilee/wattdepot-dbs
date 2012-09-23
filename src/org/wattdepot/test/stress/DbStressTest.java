package org.wattdepot.test.stress;

import static org.wattdepot.server.ServerProperties.DB_IMPL_KEY;
import java.util.Date;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbBadIntervalException;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Stress test for loading and retrieving data from WattDepot. This is implemented as a JUnit test
 * so that it can be run in Eclipse.
 * 
 * @author George Lee
 */
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
public class DbStressTest extends DbStressTestHelper {
  private static final long DATA_AMOUNT = 1000000;
  private static final long DATA_INTERVAL = 15000;
  private static final long startDate = new Date().getTime();
  private static final String TEST_SOURCE_NAME = "hale-test";
  private static final long TEST_ITERATIONS = 1000;

  /**
   * Sets up the server and inserts data into the current database implementation.
   * 
   * @throws Exception if there is an error setting the server up.
   */
  @Before
  public void initializeServer() throws Exception {
    DbStressTest.server = Server.newTestInstance();
    DbStressTest.manager =
        new DbManager(server, server.getServerProperties().get(DB_IMPL_KEY), true);
    // Initialize test source(s).
    User user = makeTestUser("test@test.org");
    DbStressTest.manager.storeUser(user);
    final Source source = createTestSource(TEST_SOURCE_NAME, user, true, false);
    DbStressTest.manager.storeSource(source);
    
    SensorData testData;

    // Insert data serially
    Date testStart = new Date();
    for (int i = 0; i < DATA_AMOUNT; i++) {
      testData =
          createSensorData(Tstamp.makeTimestamp(DbStressTest.startDate + (i * DATA_INTERVAL)),
              source);
      DbStressTest.manager.storeSensorData(testData);
    }

    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format("Time to insert %d rows serially: %.1f ms%n", DATA_AMOUNT / 2, msElapsed);
  }
  
  /**
   * Randomly retrieves a row of data from the database.
   */
  @Override
  @Test
  public void testRandomRetrieval() {
    Random random = new Random();
    long offset = 0;
    Date testStart = new Date();
    random.setSeed(testStart.getTime());
    for (int i = 0; i < TEST_ITERATIONS; i++) {
      offset = (random.nextLong() * DATA_INTERVAL) % DATA_AMOUNT;
      manager
          .getSensorData(TEST_SOURCE_NAME, Tstamp.makeTimestamp(DbStressTest.startDate + offset));
    }
    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format("Time to randomly query the database %d times: %.1f ms%n", TEST_ITERATIONS,
        msElapsed);
  }

  /**
   * Randomly retrieves a day's worth of information from WattDepot.
   * 
   * @throws DbBadIntervalException if an invalid interval is specified (should not happen).
   */
  @Override
  @Test
  public void testRandomDailyIndexes() throws DbBadIntervalException {
    Random random = new Random();
    long startOffset = 0;
    long timePeriod = (60000 / DATA_INTERVAL) * 60 * 24; // Day's worth of data.

    Date testStart = new Date();
    random.setSeed(testStart.getTime());
    for (int i = 0; i < TEST_ITERATIONS; i++) {
      startOffset = (random.nextLong() * DATA_INTERVAL) % DATA_AMOUNT;
      if (startOffset < timePeriod) {
        manager.getSensorDataIndex(TEST_SOURCE_NAME, Tstamp.makeTimestamp(startOffset),
            Tstamp.makeTimestamp(startOffset + timePeriod));
      }
      else {
        manager.getSensorDataIndex(TEST_SOURCE_NAME,
            Tstamp.makeTimestamp(startOffset - timePeriod), Tstamp.makeTimestamp(startOffset));
      }
    }

    Date testEnd = new Date();
    double msElapsed = testEnd.getTime() - testStart.getTime();
    System.out.format(
        "Time to randomly retrieve indexes of size %d from the database %d times: %.1f ms%n",
        timePeriod, TEST_ITERATIONS, msElapsed);
  }
}
