package org.wattdepot.datainput;

import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.IOException;
import javax.xml.datatype.XMLGregorianCalendar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.MiscClientException;
import org.wattdepot.client.NotAuthorizedException;
import org.wattdepot.client.ResourceNotFoundException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Creates bogus sensor data periodically and sends it to a Source in a WattDepot server. Intended
 * to be used only to create rapidly-changing data for testing monitoring clients.
 * 
 * @author Robert Brewer
 */
public class ExampleStreamingSensor {

  /** Name of the property file containing essential preferences. */
  protected DataInputClientProperties properties;
  /** The name of the source to be monitored. */
  private String sourceName;
  /** The rate at which to poll the source for new data. */
  private int updateRate;
  /** Whether to display debugging data. */
  private boolean debug;

  /** Name of this tool. */
  private static final String toolName = "ExampleStreamingSensor";
  /** The default polling rate, in seconds. */
  private static final int DEFAULT_UPDATE_RATE = 10;
  /** The polling rate that indicates that it needs to be set to a default. */
  private static final int UPDATE_RATE_SENTINEL = 0;
  /** The minimum power level for the sawtooth curve. */
  private static final double BASE_POWER_LEVEL = 1000.0;
  /** The increment for each step of the sawtooth curve. */
  private static final double POWER_LEVEL_STEP = 100.0;
  /** The cutoff power level for the sawtooth curve. */
  private static final double CUTOFF_POWER_LEVEL = BASE_POWER_LEVEL + (POWER_LEVEL_STEP * 10);

  /** Making PMD happy. */
  private static final String REQUIRED_PARAMETER_ERROR_MSG =
      "Required parameter %s not found in properties.%n";

  /**
   * Creates the new streaming sensor.
   * 
   * @param propertyFilename Name of the file to read essential properties from.
   * @param sourceName The name of the source to be monitored.
   * @param updateRate The rate at which to send new data to the source, in seconds.
   * @param debug If true then display new sensor data when sending it.
   * @throws IOException If the property file cannot be found or read.
   */
  public ExampleStreamingSensor(String propertyFilename, String sourceName, int updateRate,
      boolean debug) throws IOException {
    if (propertyFilename == null) {
      this.properties = new DataInputClientProperties();
    }
    else {
      this.properties = new DataInputClientProperties(propertyFilename);
    }
    this.sourceName = sourceName;
    if (updateRate < UPDATE_RATE_SENTINEL) {
      // Got a bogus updateRate, set to sentinel value to ensure a default is picked
      this.updateRate = UPDATE_RATE_SENTINEL;
    }
    else {
      this.updateRate = updateRate;
    }
    this.debug = debug;
    if (this.debug) {
      System.err.println(this.properties.echoProperties());
    }
  }

  /**
   * Creates bogus sensor data periodically and sends it to a Source in a WattDepot server.
   * 
   * @return False if there is a fatal problem. Otherwise will never return.
   * @throws InterruptedException If some other thread interrupts our sleep.
   */
  public boolean process() throws InterruptedException {
    String wattDepotURI = this.properties.get(WATTDEPOT_URI_KEY);
    if (wattDepotURI == null) {
      System.err.format(REQUIRED_PARAMETER_ERROR_MSG, WATTDEPOT_URI_KEY);
      return false;
    }
    String wattDepotUsername = this.properties.get(WATTDEPOT_USERNAME_KEY);
    if (wattDepotUsername == null) {
      System.err.format(REQUIRED_PARAMETER_ERROR_MSG, WATTDEPOT_USERNAME_KEY);
      return false;
    }
    String wattDepotPassword = this.properties.get(WATTDEPOT_PASSWORD_KEY);
    if (wattDepotPassword == null) {
      System.err.format(REQUIRED_PARAMETER_ERROR_MSG, WATTDEPOT_PASSWORD_KEY);
      return false;
    }

    String sourceUri = Source.sourceToUri(this.sourceName, wattDepotURI);

    WattDepotClient client =
        new WattDepotClient(wattDepotURI, wattDepotUsername, wattDepotPassword);
    Source source;
    if (client.isAuthenticated()) {
      try {
        source = client.getSource(this.sourceName);
      }
      catch (NotAuthorizedException e) {
        System.err.format("Source %s does not allow public/anonymous access. Aborting.%n",
            this.sourceName);
        return false;
      }
      catch (ResourceNotFoundException e) {
        System.err.format("Source %s does not exist on server. Aborting.%n", this.sourceName);
        return false;
      }
      catch (BadXmlException e) {
        System.err.println("Received bad XML from server, which is weird. Aborting.");
        return false;
      }
      catch (MiscClientException e) {
        System.err.println("Had problems retrieving source from server, which is weird. Aborting.");
        return false;
      }
      if (this.updateRate == UPDATE_RATE_SENTINEL) {
        // Need to pick a reasonable default pollingInterval
        // Check the polling rate specified in the source
        String updateIntervalString = source.getProperty(Source.UPDATE_INTERVAL);
        if (updateIntervalString == null) {
          // no update interval, so just use hard coded default
          this.updateRate = DEFAULT_UPDATE_RATE;
        }
        else {
          int possibleInterval;
          try {
            possibleInterval = Integer.valueOf(updateIntervalString);
            if (possibleInterval > DEFAULT_UPDATE_RATE) {
              // Sane interval, so use it
              this.updateRate = possibleInterval;
            }
            else {
              // Bogus interval, so use hard coded default
              this.updateRate = DEFAULT_UPDATE_RATE;
            }
          }
          catch (NumberFormatException e) {
            System.err.println("Unable to parse updateInterval, using default value: "
                + DEFAULT_UPDATE_RATE);
            // Bogus interval, so use hard coded default
            this.updateRate = DEFAULT_UPDATE_RATE;
          }
        }
      }

      SensorData data;
      double powerValue = BASE_POWER_LEVEL;
      while (true) {
        // Create SensorData object
        XMLGregorianCalendar timestamp = Tstamp.makeTimestamp();
        Property powerConsumed =
            new Property(SensorData.POWER_CONSUMED, Double.toString(powerValue));
        data = new SensorData(timestamp, toolName, sourceUri, powerConsumed);

        // Store SensorData in WattDepot server
        try {
          client.storeSensorData(data);
        }
        catch (Exception e) {
          System.out.println("Unable to store sensor data.");
        }
        if (debug) {
          System.out.println(data);
        }
        if (powerValue >= CUTOFF_POWER_LEVEL) {
          powerValue = BASE_POWER_LEVEL;
        }
        else {
          powerValue += POWER_LEVEL_STEP;
        }
        Thread.sleep(updateRate * 1000);
      }
    }
    else {
      System.err.format("Invalid credentials for source %s. Aborting.", this.sourceName);
      return false;
    }
  }

  /**
   * Processes command line arguments, creates the ExampleStreamingSensor object and starts sending.
   * 
   * @param args command line arguments.
   * @throws InterruptedException If some other thread interrupts our sleep.
   */
  public static void main(String[] args) throws InterruptedException {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("p", "propertyFilename", true, "Filename of property file");
    options
        .addOption("s", "source", true, "Name of the source to send data to, ex. \"foo-source\"");
    options.addOption("u", "updateRate", true,
        "The rate at which to send new data to the source, in seconds. If not specified, will "
            + "default to value of source's updateInterval property, or " + DEFAULT_UPDATE_RATE
            + " seconds if source has no such property");
    options.addOption("d", "debug", false, "Displays sensor data as it is sent to the server.");

    CommandLine cmd = null;
    String propertyFilename = null, sourceName = null;
    int updateRate = UPDATE_RATE_SENTINEL;
    boolean debug = false;

    CommandLineParser parser = new PosixParser();
    HelpFormatter formatter = new HelpFormatter();
    try {
      cmd = parser.parse(options, args);
    }
    catch (ParseException e) {
      System.err.println("Command line parsing failed. Reason: " + e.getMessage() + ". Exiting.");
      System.exit(1);
    }

    if (cmd.hasOption("h")) {
      formatter.printHelp(toolName, options);
      System.exit(0);
    }
    if (cmd.hasOption("p")) {
      propertyFilename = cmd.getOptionValue("p");
    }
    else {
      System.out.println("No property file name provided, using default.");
    }
    if (cmd.hasOption("s")) {
      sourceName = cmd.getOptionValue("s");
    }
    else {
      System.err.println("Required Source name parameter not provided, exiting.");
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    if (cmd.hasOption("u")) {
      String updateRateString = cmd.getOptionValue("u");
      // Make an int out of the command line argument
      try {
        updateRate = Integer.parseInt(updateRateString);
      }
      catch (NumberFormatException e) {
        System.err.println("Provided updateRate does not appear to be a number. Bad format?");
        formatter.printHelp(toolName, options);
        System.exit(1);
      }
      // Check rate for sanity
      if (updateRate < 1) {
        System.err.println("Provided updateRate is < 1, which is not allowed.");
        formatter.printHelp(toolName, options);
        System.exit(1);
      }
    }
    debug = cmd.hasOption("d");

    if (debug) {
      System.out.println("propertyFilename: " + propertyFilename);
      System.out.println("sourceName: " + sourceName);
      System.out.println("updateRate: " + updateRate);
      System.out.println("debug: " + debug);
      System.out.println();
    }

    // Actually create the input client
    ExampleStreamingSensor streamingSensor = null;
    try {
      streamingSensor = new ExampleStreamingSensor(propertyFilename, sourceName, updateRate, debug);
    }
    catch (IOException e) {
      System.err.println("Unable to read properties file, terminating.");
      System.exit(2);
    }
    // Just do it
    if (streamingSensor != null) {
      streamingSensor.process();
    }
  }
}
