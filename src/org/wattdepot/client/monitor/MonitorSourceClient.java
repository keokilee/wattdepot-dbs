package org.wattdepot.client.monitor;

import java.util.Date;
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
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;

/**
 * Monitors the latest SensorData from a selected Source, and prints out the SensorData object. The
 * client checks for new data periodically.
 * 
 * @author Robert Brewer
 */
public class MonitorSourceClient {

  /** The URI of the server to be monitored. */
  private String uri;
  /** The name of the source to be monitored. */
  private String sourceName;
  /** The rate at which to poll the source for new data. */
  private int pollingRate;
  /** If fetched latest data is same as last fetch, do we display it? */
  private boolean onlyDisplayNewData;

  /** Name of this tool. */
  private static final String toolName = "MonitorSourceClient";
  /** The default polling rate, in seconds. */
  private static final int DEFAULT_POLLING_RATE = 10;
  /** The polling rate that indicates that it needs to be set to a default. */
  private static final int POLLING_RATE_SENTINEL = 0;

  /**
   * Creates the new monitor client.
   * 
   * @param uri The URI of the WattDepot server.
   * @param sourceName The name of the source to be monitored.
   * @param pollingRate The rate at which to poll the source for new data, in seconds.
   * @param onlyDisplayNewData If true then if fetched latest data is same as last fetch, do not
   * display it.
   */
  public MonitorSourceClient(String uri, String sourceName, int pollingRate,
      boolean onlyDisplayNewData) {
    this.uri = uri;
    this.sourceName = sourceName;
    if (pollingRate < POLLING_RATE_SENTINEL) {
      // Got a bogus polling rate, set to sentinel value to ensure a default is picked
      this.pollingRate = POLLING_RATE_SENTINEL;
    }
    else {
      this.pollingRate = pollingRate;
    }
    this.onlyDisplayNewData = onlyDisplayNewData;
  }

  /**
   * Actually starts the monitoring of the source. Note that this method will only return if it
   * encounters a fatal error.
   * 
   * @throws InterruptedException If some other thread interrupts our sleep.
   */
  public void monitor() throws InterruptedException {
    WattDepotClient client = new WattDepotClient(this.uri);
    Source source;
    if (client.isHealthy()) {
      try {
        source = client.getSource(this.sourceName);
      }
      catch (NotAuthorizedException e) {
        System.err.format("Source %s does not allow public/anonymous access. Aborting.%n",
            this.sourceName);
        return;
      }
      catch (ResourceNotFoundException e) {
        System.err.format("Source %s does not exist on server. Aborting.%n", this.sourceName);
        return;
      }
      catch (BadXmlException e) {
        System.err.println("Received bad XML from server, which is weird. Aborting.");
        return;
      }
      catch (MiscClientException e) {
        System.err.println("Had problems retrieving source from server, which is weird. Aborting.");
        return;
      }
      if (this.pollingRate == POLLING_RATE_SENTINEL) {
        // Need to pick a reasonable default pollingInterval
        // Check the polling rate specified in the source
        String updateIntervalString = source.getProperty(Source.UPDATE_INTERVAL);
        if (updateIntervalString == null) {
          // no update interval, so just use hard coded default
          this.pollingRate = DEFAULT_POLLING_RATE;
        }
        else {
          try {
            int possibleInterval = Integer.valueOf(updateIntervalString);
            if (possibleInterval > POLLING_RATE_SENTINEL) {
              // Sane interval, so use it
              this.pollingRate = possibleInterval;
            }
            else {
              // Bogus interval, so use hard coded default
              this.pollingRate = DEFAULT_POLLING_RATE;
            }
          }
          catch (NumberFormatException e) {
            System.err.println("Unable to parse pollingRate, using default value: "
                + DEFAULT_POLLING_RATE);
            // Bogus interval, so use hard coded default
            this.pollingRate = DEFAULT_POLLING_RATE;
          }
        }
      }
      // Start loop to display latest sensor data
      SensorData lastData = null, data = null;
      while (true) {
        // Save previous fetched data
        lastData = data;
        // Fetch latest data
        try {
          data = client.getLatestSensorData(this.sourceName);
        }
        catch (NotAuthorizedException e) {
          // Any lack of authorization should have been caught when fetching source, but whatever
          System.err.format("Source %s does not allow public/anonymous access. Aborting.%n",
              this.sourceName);
          return;
        }
        catch (ResourceNotFoundException e) {
          System.err
              .format("Source %s does not have any sensor data. Aborting.%n", this.sourceName);
          return;
        }
        catch (BadXmlException e) {
          System.err.println("Received bad XML from server, which is weird. Aborting.");
          return;
        }
        catch (MiscClientException e) {
          System.err
              .println("Had problems retrieving source from server, which is weird. Aborting.");
          return;
        }

        if (this.onlyDisplayNewData && data.equals(lastData)) {
          // Don't display anything because data was the same and we aren't supposed to display
          continue;
        }
        else {
          System.out.format("Last checked: %s%n", new Date());
          System.out.format("Data received: %s%n", data.getTimestamp());
          System.out.format("properties: %s%n", data.getProperties());
          System.out.println();
        }
        Thread.sleep(pollingRate * 1000);
      }
    }
    else {
      System.err.println("Unable to connect to WattDepot server. Aborting.");
      return;
    }
  }

  /**
   * Processes command line arguments, creates the MonitorSourceClient object and starts monitoring.
   * 
   * @param args command line arguments.
   * @throws InterruptedException If some other thread interrupts our sleep.
   */
  public static void main(String[] args) throws InterruptedException {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("u", "uri", true,
        "URI of WattDepot server, ex. \"http://server.wattdepot.org:8182/wattdepot/\".");
    options.addOption("s", "source", true,
        "Name of the source to retrieve data from, ex. \"foo-source\"");
    options.addOption("p", "pollingRate", true,
        "The rate at which to poll the source for new data, in seconds. If not specified, will "
            + "default to value of source's updateInterval property, or " + DEFAULT_POLLING_RATE
            + " seconds if source has no such property");
    options.addOption("d", "onlyDisplayNewData", false,
        "do not display fetched latest data if it is the same as last fetched data");

    CommandLine cmd = null;
    String uri = null, sourceName = null;
    int pollingRate = POLLING_RATE_SENTINEL;
    boolean onlyDisplayNewData = false;

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
    if (cmd.hasOption("u")) {
      uri = cmd.getOptionValue("u");
    }
    else {
      System.err.println("Required URI parameter not provided, exiting.");
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    if (cmd.hasOption("s")) {
      sourceName = cmd.getOptionValue("s");
    }
    else {
      System.err.println("Required Source name parameter not provided, exiting.");
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    if (cmd.hasOption("p")) {
      String pollingRateString = cmd.getOptionValue("p");
      // Make an int out of the command line argument
      try {
        pollingRate = Integer.parseInt(pollingRateString);
      }
      catch (NumberFormatException e) {
        System.err.println("Provided pollingRate does not appear to be a number. Bad format?");
        formatter.printHelp(toolName, options);
        System.exit(1);
      }
      // Check interval for sanity
      if (pollingRate < 1) {
        System.err.println("Provided pollingRate is < 1, which is not allowed.");
        formatter.printHelp(toolName, options);
        System.exit(1);
      }
    }
    onlyDisplayNewData = cmd.hasOption("d");

    // Results of command line processing, should probably be commented out
    System.out.println("uri: " + uri);
    System.out.println("sourceName: " + sourceName);
    System.out.println("pollingRate: " + pollingRate);
    System.out.println("onlyDisplayNewData: " + onlyDisplayNewData);
    System.out.println();

    // Actually create the input client
    MonitorSourceClient monitorClient = null;
    monitorClient = new MonitorSourceClient(uri, sourceName, pollingRate, onlyDisplayNewData);
    // Just do it
    if (monitorClient != null) {
      monitorClient.monitor();
    }
  }
}
