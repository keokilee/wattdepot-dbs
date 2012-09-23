package org.wattdepot.test.stress;

import static org.wattdepot.server.ServerProperties.DB_IMPL_KEY;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Stress test for storing and retrieving data that runs using parallel threads. Uses JUnit so that
 * it can be run in parallel.
 * 
 * @author George Lee
 */
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
public class ParallelDbStressTest extends DbStressTestHelper {
  private static final int DATA_AMOUNT = 100000;
  private static final int DATA_INTERVAL = 15000;
  private static long startDate = 0;
  private static final String TEST_SOURCE_NAME = "hale-test";
  private static final long TEST_ITERATIONS = 1000;
  private static final int THREADS = 4;
  
  static {
    try {
      ParallelDbStressTest.server = Server.newTestInstance();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    ParallelDbStressTest.manager =
        new DbManager(server, server.getServerProperties().get(DB_IMPL_KEY), true);
  }
  
  /**
   * Sets up the server and inserts data into the current database implementation.
   * 
   * @throws Exception if the server cannot be set up.
   */
  @Before
  public void initializeServer() throws Exception {
    // Initialize test source(s).
    User user = makeTestUser("test@test.org");
    ParallelDbStressTest.manager.storeUser(user);
    final Source source = createTestSource(TEST_SOURCE_NAME, user, true, false);
    ParallelDbStressTest.manager.storeSource(source);

    Date initialDate = new Date();
    ParallelDbStressTest.startDate = initialDate.getTime();

    // Insert data in parallel
    // Set up threads
    List<ManagerThread> threads = new ArrayList<ManagerThread>();
    for (int j = 0; j < THREADS; j++) {
      threads.add(new ManagerThread(server, j) {
        @Override
        public void run() {
          long start = (DATA_AMOUNT * id) / (THREADS * 2);
          long end = start + (DATA_AMOUNT / (THREADS * 2));
          for (long i = start; i < end; i++) {
            try {
              SensorData testData =
                  createSensorData(
                      Tstamp.makeTimestamp(ParallelDbStressTest.startDate + (i * DATA_INTERVAL)),
                      source);
              ParallelDbStressTest.manager.storeSensorData(testData);
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }
    double msElapsed = this.runThreads(threads, 500);
    System.out.format("Time to insert %d rows in parallel: %.1f ms%n", DATA_AMOUNT, msElapsed);
  }

  /**
   * Randomly retrieves a row of data from the database using parallel threads.
   * 
   * @throws Exception if there is an issue with the threads.
   */
  @Override
  @Test
  public void testRandomRetrieval() throws Exception {
    List<ManagerThread> threads = new ArrayList<ManagerThread>();
    for (int j = 0; j < THREADS; j++) {
      threads.add(new ManagerThread(server, j) {
        @Override
        public void run() {
          Random random = new Random();
          long offset = 0;
          random.setSeed(new Date().getTime());
          for (int i = 0; i < TEST_ITERATIONS / THREADS; i++) {
            offset = (random.nextLong() * DATA_INTERVAL) % DATA_AMOUNT;
            manager.getSensorData(TEST_SOURCE_NAME, Tstamp.makeTimestamp(startDate + offset));
          }
        }
      });
    }
    double msElapsed = this.runThreads(threads, 10);
    System.out.format("Time to randomly query the database %d times: %.1f ms%n", TEST_ITERATIONS,
        msElapsed);
  }
  
  /**
   * Randomly retrieves a day's worth of database using parallel threads.
   * 
   * @throws Exception if there is an issue with the threads.
   */
  @Override
  @Test
  public void testRandomDailyIndexes() throws Exception {
    final long timePeriod = (60000 / DATA_INTERVAL) * 60 * 24; // Day's worth of data.

    List<ManagerThread> threads = new ArrayList<ManagerThread>();
    for (int j = 0; j < THREADS; j++) {
      threads.add(new ManagerThread(server, j) {
        @Override
        public void run() {
          Random random = new Random();
          long startOffset = 0;
          random.setSeed(new Date().getTime());
          for (int i = 0; i < TEST_ITERATIONS / THREADS; i++) {
            startOffset = (random.nextLong() * DATA_INTERVAL) % DATA_AMOUNT;
            try {
              if (startOffset < timePeriod) {
                manager.getSensorDataIndex(TEST_SOURCE_NAME, Tstamp.makeTimestamp(startOffset),
                    Tstamp.makeTimestamp(startOffset + timePeriod));
              }
              else {
                manager.getSensorDataIndex(TEST_SOURCE_NAME,
                    Tstamp.makeTimestamp(startOffset - timePeriod),
                    Tstamp.makeTimestamp(startOffset));
              }
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }

    double msElapsed = this.runThreads(threads, 10);
    System.out.format(
        "Time to randomly retrieve indexes of size %d from the database %d times: %.1f ms%n",
        timePeriod, TEST_ITERATIONS, msElapsed);
  }

  /**
   * Helper method execute the provided threads.
   * 
   * @param threads The threads to execute.
   * @param delay The delay to use when waiting for threads.
   * @return The time taken for all threads to finish execution.
   * @throws Exception if there are issues with the threads.
   */
  private double runThreads(List<ManagerThread> threads, int delay) throws Exception {
    Date testStart = new Date();
    for (ManagerThread thread : threads) {
      thread.start();
    }
    // Block until threads are done
    boolean isComplete = false;
    while (!isComplete) {
      isComplete = true;
      for (ManagerThread thread : threads) {
        isComplete = isComplete && !thread.isAlive();
      }
      // Prevent busy wait.
      if (!isComplete) {
        Thread.sleep(delay);
      }
    }
    Date testEnd = new Date();
    return testEnd.getTime() - testStart.getTime();
  }
}

