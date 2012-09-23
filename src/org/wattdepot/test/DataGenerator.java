package org.wattdepot.test;

import static org.wattdepot.server.ServerProperties.ADMIN_EMAIL_KEY;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SubSources;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Generates test data for performance and stress testing.
 * 
 * @author Robert Brewer
 */
public class DataGenerator {
  /** The DbManager used to input the data. */
  protected DbManager dbManager;

  /** Virtual source name. */
  public static final String source11Name = "virtualSource";
  /** Virtual source containing all other sources. */
  public Source virtualSource;
  /** Array containing all the names of the non-virtual sources. */
  private static final String[] sourceNames =
      { "source01", "source02", "source03", "source04", "source05", "source06", "source07",
          "source08", "source09", "source10" };
  /** The number of non-virtual sources used for testing. */
  public static final int NUM_SOURCES = sourceNames.length;
  /** The test Sources in URI form. */
  public String[] sourceURIs = new String[NUM_SOURCES];
  /** Array containing all the of the non-virtual sources. */
  public static final Source[] sources = new Source[NUM_SOURCES];
  /** The name used for the tool when storing SensorData. */
  private static final String toolName = "DataGenerator";

  /**
   * Creates the new object, sets up the required sources.
   * 
   * @param dbManager The dbManager to store the sources in.
   * @param adminUserUri The URI of the admin user.
   * @param server The server (to construct URIs).
   */
  public DataGenerator(DbManager dbManager, String adminUserUri, Server server) {
    this.dbManager = dbManager;
    SubSources subSources = new SubSources();
    for (int i = 0; i < NUM_SOURCES; i++) {
      Source source = new Source(sourceNames[i], adminUserUri, true);
      source.addProperty(new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true"));
      sources[i] = source;
      sourceURIs[i] = source.toUri(server);
      subSources.getHref().add(sourceURIs[i]);
      this.dbManager.storeSource(source);
    }
    virtualSource =
        new Source(source11Name, adminUserUri, true, true, "", "", "", null, subSources);
    this.dbManager.storeSource(virtualSource);
  }

  /**
   * Stores lots of test data into the database.
   * 
   * @param startTime The first timestamp to generate data for.
   * @param endTime The last timestamp to generate data for.
   * @param rate The rate in minutes at which to generate successive data points.
   * 
   * @return The number of items stored.
   */
  public int storeData(XMLGregorianCalendar startTime, XMLGregorianCalendar endTime, int rate) {
    long intervalMilliseconds;
    long rangeLength = Tstamp.diff(startTime, endTime);
    long minutesToMilliseconds = 60L * 1000L;

    if (rate <= 0) {
      return 0;
    }
    if (rangeLength <= 0) {
      // either startTime == endTime, or startTime > endTime
      return 0;
    }
    else if ((rate * minutesToMilliseconds) > rangeLength) {
      return 0;
    }
    else {
      // got a good interval
      intervalMilliseconds = rate * minutesToMilliseconds;
    }
    // DEBUG
    // System.out.format("%nstartTime=%s, endTime=%s, interval=%d min%n", startTime, endTime,
    // intervalMilliseconds / minutesToMilliseconds);

    // Build list of timestamps, starting with startTime, separated by intervalMilliseconds
    XMLGregorianCalendar timestamp = startTime;
    int j = 0;
    SensorData data;
    Properties props;
    int power;
    // Store energy counter per source across loop iterations
    double totalEnergyGenerated[] = {0, 1234.0, 2345, 3456, 4567, 5678, 6789, 7890, 8901, 9012};
    int count = 0;
    while (Tstamp.lessThan(timestamp, endTime)) {
      for (int i = 0; i < NUM_SOURCES; i++) {
        props = new Properties();
        power = j * (i + 1) * 100;
        // energy counter increased by power * rate, converted to Wh
        totalEnergyGenerated[i] += power * rate / 60.0;
        props.getProperty().add(new Property(SensorData.POWER_GENERATED, Integer.toString(power)));
        props.getProperty()
            .add(
                new Property(SensorData.ENERGY_GENERATED_TO_DATE, Double
                    .toString(totalEnergyGenerated[i])));
        data = new SensorData(timestamp, toolName, sourceURIs[i], props);
//        System.out.println(data); // DEBUG
        this.dbManager.storeSensorData(data);
      }
      // System.out.format("timestamp=%s%n", timestamp); // DEBUG
      timestamp = Tstamp.incrementMilliseconds(timestamp, intervalMilliseconds);
      // Keep ratcheting up j until we reach 100, then reset to 0
      j = (j < 100) ? j + 10 : 0;
      // Keep track of how many rows of sensor data we stored.
      count += NUM_SOURCES;
    }
    
    return count;
  }

  /**
   * Returns the name of the source given by the index. Basically created to eliminate a FindBugs
   * warning.
   * 
   * @param index A zero-based number indicating which source name is desired.
   * @return The name of the source as a String.
   */
  public static String getSourceName(int index) {
    if ((index >= 0) && (index <= sourceNames.length)) {
      return sourceNames[index];
    }
    else {
      return null;
    }
  }

  /**
   * Used to test the data generator.
   * 
   * @param args Command line arguments.
   * @throws Exception If there are problems.
   */
  public static void main(String[] args) throws Exception {
    Server server = Server.newInstance();
    String adminEmail = server.getServerProperties().get(ADMIN_EMAIL_KEY);
    DbManager dbManager = (DbManager) server.getContext().getAttributes().get("DbManager");
    String adminUserUri = dbManager.getUser(adminEmail).toUri(server);
    DataGenerator test = new DataGenerator(dbManager, adminUserUri, server);
    test.storeData(Tstamp.makeTimestamp("2010-01-08T00:00:00.000-10:00"), Tstamp
        .makeTimestamp("2010-01-09T00:00:00.000-10:00"), 5);
  }

}
