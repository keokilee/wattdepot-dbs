package org.wattdepot.datainput;

import static org.wattdepot.datainput.DataInputClientProperties.BMO_AS_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.BMO_DB_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.BMO_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.BMO_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.BMO_USERNAME_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.IOException;
import java.util.Arrays;
import javax.xml.datatype.XMLGregorianCalendar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.MiscClientException;
import org.wattdepot.client.NotAuthorizedException;
import org.wattdepot.client.ResourceNotFoundException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.logger.WattDepotUserHome;
import org.wattdepot.util.tstamp.Tstamp;
import au.com.bytecode.opencsv.CSVReader;

/**
 * Retrieves data from Building Manager Online (website for power meter data provided by Obvius),
 * writes the data into a TSV file (temporary kludge since no persistence in the server yet),
 * creates a SensorData object for each line of the table, and sends the SensorData objects off to a
 * WattDepot server.
 * 
 * @author Robert Brewer
 */
public class BMOSensor {

  /** Making PMD happy. */
  private static final String REQUIRED_PARAMETER_ERROR_MSG =
      "Required parameter %s not found in properties.%n";
  /** Conversion factor for milliseconds per minute. */
  private static final long MILLISECONDS_PER_MINUTE = 60L * 1000;
  /** Name of the property file containing essential preferences. */
  protected DataInputClientProperties properties;
  /** Name of Source to send data to. */
  protected String sourceName;
  /** Number of the meter to download data for. */
  protected String meterNumber;
  /** Starting point for initial data download. */
  protected XMLGregorianCalendar startTimestamp;
  /** Whether to display debugging data. */
  private boolean debug; // NOPMD
  /** Name of the application on the command line. */
  protected static final String toolName = "BMOSensor";
  /** The parser used to turn rows into SensorData objects. */
  protected RowParser parser;
  /** The client used to send SensorData to WattDepot server. */
  protected WattDepotClient client;
  /** The interval at which to query BMO for new sensor data, in minutes. */
  protected int interval;

  /**
   * Creates the new BMOSensor.
   * 
   * @param propertyFilename Name of the file to read essential properties from. Defaults to
   * ~/.wattdepot/client/datainput.properties
   * @param sourceName name of the Source the sensor data should be sent to.
   * @param meterNumber The number of the meter data is to be read from.
   * @param startTimestamp Starting point for data download. If null, then latest data from
   * WattDepot will be used as starting point.
   * @param interval Interval (in minutes) at which to sample BMO data (sleeping in-between runs).
   * @param debug If true then display new sensor data when sending it.
   * @throws IOException If the property file cannot be found.
   */
  public BMOSensor(String propertyFilename, String sourceName, String meterNumber,
      XMLGregorianCalendar startTimestamp, int interval, boolean debug) throws IOException {
    this.debug = debug;
    if (propertyFilename == null) {
      // Use default property file name
      this.properties =
          new DataInputClientProperties(WattDepotUserHome.getHomeString()
              + "/.wattdepot/client/datainput.properties");
    }
    else {
      this.properties = new DataInputClientProperties(propertyFilename);
    }
    if (this.debug) {
      System.err.println(this.properties.echoProperties());
    }
    this.sourceName = sourceName;
    this.meterNumber = meterNumber;
    this.startTimestamp = startTimestamp;
    this.interval = interval;
  }

  /**
   * Does the work of inputting the data into WattDepot.
   * 
   * @return True if the data could be successfully input, false otherwise.
   * @throws InterruptedException If some other thread interrupts our sleep.
   */
  public boolean process() throws InterruptedException {
    // Extract all the properties we need, and fail if any critical ones are missing.
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
    String bmoURI = this.properties.get(BMO_URI_KEY);
    if (bmoURI == null) {
      System.err.format(REQUIRED_PARAMETER_ERROR_MSG, BMO_URI_KEY);
      return false;
    }
    String bmoUsername = this.properties.get(BMO_USERNAME_KEY);
    if (bmoUsername == null) {
      System.err.format(REQUIRED_PARAMETER_ERROR_MSG, BMO_USERNAME_KEY);
      return false;
    }
    String bmoPassword = this.properties.get(BMO_PASSWORD_KEY);
    if (bmoPassword == null) {
      System.err.format(REQUIRED_PARAMETER_ERROR_MSG, BMO_PASSWORD_KEY);
      return false;
    }
    String bmoDB = this.properties.get(BMO_DB_KEY);
    if (bmoDB == null) {
      System.err.format(REQUIRED_PARAMETER_ERROR_MSG, BMO_DB_KEY);
      return false;
    }
    String bmoAS = this.properties.get(BMO_AS_KEY);
    if (bmoAS == null) {
      System.err.format(REQUIRED_PARAMETER_ERROR_MSG, BMO_AS_KEY);
      return false;
    }

    this.parser = new VerisRowParser(toolName, wattDepotURI, this.sourceName);
    this.client = new WattDepotClient(wattDepotURI, wattDepotUsername, wattDepotPassword);

    if (client.isAuthenticated()) {
      try {
        // Check first if source exists by just fetching it
        client.getSource(this.sourceName);
      }
      catch (NotAuthorizedException e) {
        System.err.format("You do not have access to the source %s. Aborting.%n", this.sourceName);
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

      // If there is sensor data in the source, use the latest value as the startTimestamp and
      // ignore anything provided on command line
      try {
        this.startTimestamp = client.getLatestSensorData(this.sourceName).getTimestamp();
      }
      catch (NotAuthorizedException e) {
        System.err.format("You do not have access to the source %s. Aborting.%n", this.sourceName);
        return false;
      }
      catch (ResourceNotFoundException e) {
        // The source has no sensor data, so if the user didn't provide a startTimestamp then
        // we have to bail
        if (this.startTimestamp == null) {
          System.err
              .format("Source %s has no sensor data, and you did not supply%na startTimestamp"
                  + " so we don't know when to start collecting data. Aborting.%n", this.sourceName);
          return false;
        }
      }
      catch (BadXmlException e) {
        System.err.println("Received bad XML from server, which is weird. Aborting.");
        return false;
      }
      catch (MiscClientException e) {
        System.err.println("Had problems retrieving source from server, which is weird. Aborting.");
        return false;
      }

      BMONetworkClient bmoClient =
          new BMONetworkClient(bmoURI, bmoUsername, bmoPassword, bmoDB, bmoAS);
      // We start gathering data from startTimestamp
      XMLGregorianCalendar nextStartTime = this.startTimestamp;
      if (debug) {
        System.out.println("startTimestamp: " + this.startTimestamp);
      }

      // Using do-while so there will always be one iteration even when no interval was supplied
      do {
        // next run will start from whatever the last retrieved timestamp was
        nextStartTime = processLatestBMOData(bmoClient, this.meterNumber, nextStartTime);
        if (nextStartTime == null) {
          // Something went wrong with BMO retrieval, since we should always be able to grab data
          // for
          // the last timestamp we received from BMO
          // In future, for more robust long-term fault-tolerant operation, we might want to
          // distinguish between BMO problems that should result in giving up, or just waiting and
          // trying again. Right now, we always terminate.
          return false;
        }
        if (this.interval >= 1) {
          String outputFormatString =
              (this.interval == 1) ? "Sleeping for %d minute%n" : "Sleeping for %d minutes%n";
          System.out.format(outputFormatString, this.interval);
          Thread.sleep(interval * MILLISECONDS_PER_MINUTE);
        }
      }
      while (this.interval >= 1);

      // Only reach here if the interval was not specified (one-shot mode)
      return true;
    }
    else {
      System.err.format("Invalid credentials for source %s. Aborting.", this.sourceName);
      return false;
    }
  }

  /**
   * Retrieves data from BMO for a particular meter from the provided start time to the time of
   * invocation. The resulting meter data are sent to the WattDepot server, and the last timestamp
   * received is the return value. Any data from a timestamp equal to startTimestamp is discarded
   * and not recorded.
   * 
   * This method is intended to be run periodically to collect BMO data from the last iteration. In
   * this scenario, the caller sets startTimestamp equal to the start of the period they are
   * interested in, sleeps some appropriate interval, and then calls this method again with the
   * startTimestamp set to the return value from the last call. In this way, each invocation
   * retrieves any new data since the last call. Since data equal to the startTimestamp is
   * discarded, there will be no data overlap even if this method is called more frequently than BMO
   * or the meter's sampling interval.
   * 
   * @param bmoClient The client used to retrieve data from BMO.
   * @param meterNumber The number of the meter to collect data for.
   * @param startTimestamp The start time of the period of data BMO data is to be downloaded from.
   * @return The timestamp of the last row of data retrieved. Note: it could be the same as the
   * provided startTimestamp if there is no new data available from the meter.
   */
  protected XMLGregorianCalendar processLatestBMOData(BMONetworkClient bmoClient,
      String meterNumber, XMLGregorianCalendar startTimestamp) {
    // The timestamp of the last value retrieved from BMO, to be used as a return value at the end
    XMLGregorianCalendar lastTimestamp = startTimestamp;

    // Send request to BMO for data from start timestamp to right now
    Response response =
        bmoClient.makeBMORequest(meterNumber, startTimestamp, Tstamp.makeTimestamp());
    Status status = response.getStatus();
    if (status.isSuccess()) {
      if (response.isEntityAvailable()) {
        // Finally we have content from the server
        CSVReader reader;
        try {
          // note this assumes that if we get an HTTP 200 status code then the content will be
          // tabular. Turns out BMO returns an HTML error page (with the download form) if
          // there are problems, which we don't check for yet.
          reader = new CSVReader(response.getEntity().getReader(), '\t');
        }
        catch (IOException e) {
          System.err.println("Problem reading entity body from BMO");
          return null;
        }
        // nextLine[] will hold an array of values from the current row in the file
        String[] nextLine;
        int rowsRetrieved = 0, rowsSent = 0;
        try {
          while ((nextLine = reader.readNext()) != null) {
            if (debug) {
              System.out.println("BMO row: " + Arrays.toString(nextLine));
            }
            SensorData data = this.parser.parseRow(nextLine);
            if (data == null) {
              System.err
                  .println("Got row with meter error, skipping: " + Arrays.toString(nextLine));
              rowsRetrieved++;
              // Don't update timestamp, since we don't have any way to get that in current
              // RowParser class. Probably should create a new Exception type for this. Just punt
              // for now.
            }
            else {
              if (debug) {
                System.out.println("sensordata: " + data);
              }
              rowsRetrieved++;
              XMLGregorianCalendar dataTimestamp = data.getTimestamp();
              // Make sure this timestamp is not the overlap from the last fetch
              if (!dataTimestamp.equals(startTimestamp)) {
                // Send sensor data to WattDepot
                this.client.storeSensorData(data);
                // This timestamp becomes the lastTimestamp (for now)
                lastTimestamp = dataTimestamp;
                rowsSent++;
              }
            }
          }
        }
        catch (Exception e) {
          System.err.println("Problem encountered processing BMO data: " + e.toString());
          return null;
        }
        // Finished processing all the BMO data received
        try {
          reader.close();
        }
        catch (IOException e) {
          System.err.println("Problem encountered closing BMO connection.");
        }
        String outputFormatString;
        if ((rowsRetrieved == 1) && (rowsSent == 0)) {
          outputFormatString =
              "Retrieved %d entries from BMO, sent %d entries to WattDepot"
                  + " (i.e. no new data)%n";
        }
        else {
          outputFormatString = "Retrieved %d entries from BMO, sent %d entries to WattDepot%n";
        }
        System.out.format(outputFormatString, rowsRetrieved, rowsSent);
        return lastTimestamp;
      }
      else {
        System.err.println("BMO response was successful, but contained no data?");
        return null;
      }
    }
    else if (status.isError()) {
      System.err.format("Unable to retrieve data from BMO, status code: %d %s %s%n", status
          .getCode(), status.getName(), status.getDescription());
      return null;
    }
    else {
      System.err.format(
          "Unexpected status from BMO (neither error nor success), status code: %d %s %s%n", status
              .getCode(), status.getName(), status.getDescription());
      return null;
    }
  }

  /**
   * Processes command line arguments, creates the BMOSensor object and starts data input.
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
    options.addOption("m", "meterNum", true, "Meter number to retrieve data from");
    options.addOption("t", "startTimestamp", true, "Start point for data collection in XML format"
        + " (like 2009-07-28T09:00:00.000-10:00)");
    options.addOption("i", "interval", true, "BMO sampling interval in minutes. Sensor will run"
        + " forever, fetching data from BMO after every interval.");
    options.addOption("d", "debug", false, "Displays debugging data, particularly sensor data as"
        + " it is sent to the server.");

    CommandLine cmd = null;
    String propertyFilename = null, sourceName = null, meterNumber = null;
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
    if (cmd.hasOption("s")) {
      sourceName = cmd.getOptionValue("s");
    }
    else {
      System.err.println("Required Source name parameter not provided, exiting.");
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    if (cmd.hasOption("m")) {
      meterNumber = cmd.getOptionValue("m");
    }
    else {
      System.err.println("Required meterNum parameter not provided, terminating.");
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    XMLGregorianCalendar startTimestamp = null;
    if (cmd.hasOption("t")) {
      String startTimestampString = cmd.getOptionValue("t");
      // Make a timestamp out of the command line argument
      try {
        startTimestamp = Tstamp.makeTimestamp(startTimestampString);
      }
      catch (Exception e) {
        System.err.println("Provided startTimestamp could not be parsed. Bad format?");
        formatter.printHelp(toolName, options);
        System.exit(1);
      }
    }
    int interval = -1;
    if (cmd.hasOption("i")) {
      String intervalString = cmd.getOptionValue("i");
      // Make a int interval out of the command line argument
      try {
        interval = Integer.parseInt(intervalString);
      }
      catch (NumberFormatException e) {
        System.err.println("Provided interval does not appear to be a number. Bad format?");
        formatter.printHelp(toolName, options);
        System.exit(1);
      }
      // Check interval for sanity
      if (interval < 1) {
        System.err
            .println("Provided interval is < 1, which is not allowed. Interval should probably be"
                + " much greater than 1.");
        formatter.printHelp(toolName, options);
        System.exit(1);
      }
    }
    debug = cmd.hasOption("d");

    if (debug) {
      System.out.println("propertyFilename: " + propertyFilename);
      System.out.println("sourceName: " + sourceName);
      System.out.println("meterNumber: " + meterNumber);
      System.out.println("startTimestamp: " + startTimestamp);
      System.out.println("interval: " + interval);
      System.out.println("debug: " + debug);
      System.out.println();
    }

    // Actually create the input client
    BMOSensor inputClient = null;
    try {
      inputClient =
          new BMOSensor(propertyFilename, sourceName, meterNumber, startTimestamp, interval, debug);
    }
    catch (IOException e) {
      System.err.println("Unable to read properties file, terminating.");
      System.exit(2);
    }
    // Just do it
    if ((inputClient != null) && (inputClient.process())) {
      // Note that process() will never return if the user provided an interval
      System.out.println("Successfully input data.");
      System.exit(0);
    }
    else {
      System.err.println("Problem encountered inputting data.");
      System.exit(2);
    }
  }
}