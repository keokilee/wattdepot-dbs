package org.wattdepot.client.snapshot;

import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.IOException;
import java.util.Date;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.wattdepot.client.MiscClientException;
import org.wattdepot.client.NotAuthorizedException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.datainput.DataInputClientProperties;

/**
 * Creates a snapshot of the database on the server. This is useful to ensure that a consistent
 * version of the database is available for backups.
 * 
 * @author Robert Brewer
 */
public class SnapshotClient {

  /** Name of the property file containing essential preferences. */
  protected DataInputClientProperties properties;
  /** Name of this tool. */
  private static final String toolName = "SnapshotClient";
  /** Making PMD happy. */
  private static final String REQUIRED_PARAMETER_ERROR_MSG =
      "Required parameter %s not found in properties.%n";
  /** Flag for extra debugging output. */
  private boolean debug; // NOPMD

  /**
   * Creates the new monitor client.
   * 
   * @param propertyFilename Name of the file to read essential properties from.
   * @param debug True if extra debugging output is desired.
   * @throws IOException If the property file cannot be found or read.
   */
  public SnapshotClient(String propertyFilename, boolean debug) throws IOException {
    if (propertyFilename == null) {
      this.properties = new DataInputClientProperties();
    }
    else {
      this.properties = new DataInputClientProperties(propertyFilename);
    }
    this.debug = debug;
    if (this.debug) {
      System.err.println(this.properties.echoProperties());
    }
  }

  /**
   * Attempts to create a snapshot on the server.
   * 
   * @return True if able to create a snapshot successfully, false otherwise.
   */
  public boolean makeSnapshot() {
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

    WattDepotClient client =
        new WattDepotClient(wattDepotURI, wattDepotUsername, wattDepotPassword);

    try {
      return client.makeSnapshot();
    }
    catch (NotAuthorizedException e) {
      System.err.format("Not authorized to make snapshot, message: %s.%n", e.getMessage());
      return false;
    }
    catch (MiscClientException e) {
      System.err
          .format("Unable to create snapshot for some reason, message: %s.%n", e.getMessage());
      return false;
    }
  }

  /**
   * Processes command line arguments, creates the SnapshotClient object and instructs it to create
   * the snapshot.
   * 
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("p", "propertyFilename", true, "Filename of property file");
    options.addOption("d", "debug", false, "Displays verbose log messages.");

    CommandLine cmd = null;
    String propertyFilename = null;
    boolean debug;

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
    debug = cmd.hasOption("d");

    if (debug) {
      System.out.println("propertyFilename: " + propertyFilename);
      System.out.println("debug: " + debug);
      System.out.println();
    }

    // Actually create the input client
    SnapshotClient snapshotClient = null;
    try {
      snapshotClient = new SnapshotClient(propertyFilename, debug);
    }
    catch (IOException e) {
      System.err.println("Unable to open property file. Aborting.");
      System.exit(1);
    }

    if (snapshotClient == null) {
      System.err.println("Unable to create SnapshotClient for some reason. Aborting.");
      System.exit(1);
    }
    System.out.format("%tc Snapshot started.%n", new Date());
    if (snapshotClient.makeSnapshot()) {
      System.out.format("%tc Snapshot created successfully.%n", new Date());
      System.exit(0);
    }
    else {
      System.err.format("%tc Unable to create snapshot on server. Aborting.%n", new Date());
      System.exit(2);
    }
  }
}
