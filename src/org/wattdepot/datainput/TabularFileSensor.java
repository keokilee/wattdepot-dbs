package org.wattdepot.datainput;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.MiscClientException;
import org.wattdepot.client.NotAuthorizedException;
import org.wattdepot.client.OverwriteAttemptedException;
import org.wattdepot.client.ResourceNotFoundException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import au.com.bytecode.opencsv.CSVReader;

/**
 * Reads data from files in tabular formats (delimited by some character), creates a SensorData
 * object for each line of the table, and sends the SensorData objects off to a WattDepot server.
 * 
 * @author Robert Brewer
 */
public class TabularFileSensor {

  /** Name of the file to be input. */
  protected String filename;

  /** URI of WattDepot server to send data to. */
  protected String serverUri;

  /** Name of Source to send data to. */
  protected String sourceName;

  /** Username to use when sending data to server. */
  protected String username;

  /** Password to use when sending data to server. */
  protected String password;

  /** Whether or not to skip the first row of the file. */
  protected boolean skipFirstRow;

  /** Name of the application on the command line. */
  protected static final String toolName = "TabularFileSensor";

  /** The parser used to turn rows into SensorData objects. */
  protected RowParser parser;

  /**
   * Creates the new TabularFileSensor.
   * 
   * @param filename Name of the file to read data from.
   * @param uri URI of the server to send data to (ending in "/").
   * @param sourceName name of the Source the sensor data should be sent to.
   * @param username Username used to authenticate to the server.
   * @param password Password used to authenticate to the server.
   * @param skipFirstRow If true, indicates that the first row of the file should be ignored (often
   * used for column headers, and not containing data).
   */
  public TabularFileSensor(String filename, String uri, String sourceName,
      String username, String password, boolean skipFirstRow) {
    this.filename = filename;
    this.serverUri = uri;
    this.sourceName = sourceName;
    this.username = username;
    this.password = password;
    this.skipFirstRow = skipFirstRow;
    this.parser = new VerisRowParser(toolName, serverUri, sourceName);
  }

  /**
   * Does the work of inputting the data into WattDepot.
   * 
   * @return True if the data could be successfully input, false otherwise.
   */
  public boolean process() {

    List<SensorData> list = null;

    try {
      list = readFile(this.filename, this.skipFirstRow, this.parser);
    }
    catch (IOException e) {
      return false;
    }

    if (list == null) {
      return false;
    }
    else {
      WattDepotClient client = new WattDepotClient(this.serverUri, this.username, this.password);
      return sendData(client, list);
    }
  }

  /**
   * Reads a tabular file and produces a List of SensorData to be sent to the server.
   * 
   * @param filename name of the file containing the data
   * @param skipFirstRow If true, indicates that the first row of the file should be ignored (often
   * used for column headers, and not containing data).
   * @param parser The RowParser used to turn rows into SensorData.
   * @return a List of SensorData objects created from the rows in the file.
   * @throws IOException if there is a problem encountered while reading the file.
   */
  public List<SensorData> readFile(String filename, boolean skipFirstRow, RowParser parser)
      throws IOException {
    List<SensorData> dataList = new ArrayList<SensorData>();
    CSVReader reader = null;

    try {
      if (skipFirstRow) {
        // use 4 arg constructor with skip lines = 1
        reader = new CSVReader(new FileReader(filename), '\t', CSVReader.DEFAULT_QUOTE_CHARACTER, 1);
      }
      else {
        reader = new CSVReader(new FileReader(filename), '\t');
      }
    }
    catch (FileNotFoundException e) {
      System.err.format("Data file %s not found. Exiting.%n", filename);
      return null;
    }
    String[] nextLine;
    while ((nextLine = reader.readNext()) != null) {
      // nextLine[] is an array of values from the current row in the file
      SensorData data = null;
      try {
        data = parser.parseRow(nextLine);
      }
      catch (RowParseException e) {
        System.err.println(e);
      }
      if (data != null) {
        dataList.add(data);
      }
    }
    reader.close();
    return dataList;
  }

  /**
   * Sends a List of SensorData to the WattDepot server.
   * 
   * @param client the WattDepot client to send the data via.
   * @param list the SensorData to send.
   * @return true if the data was sent successfully, false otherwise.
   */
  public boolean sendData(WattDepotClient client, List<SensorData> list) {
    if (!client.isHealthy()) {
      System.err.println("Unable to connect to server: " + this.serverUri);
      return false;
    }
    if (!client.isAuthenticated()) {
      System.err.println("Bad WattDepot server username and/or password provided.");
      return false;
    }
    for (SensorData data : list) {
      try {
        client.storeSensorData(data);
      }
      catch (NotAuthorizedException e) {
        System.err.println("Not authorized to store to source \"" + data.getSource() + "\"");
        return false;
      }
      catch (ResourceNotFoundException e) {
        System.err.println("Bad source name \"" + data.getSource() + "\" provided.");
        return false;
      }
      catch (BadXmlException e) {
        System.err.println("Server claims it received bad XML (" + e.toString() + ")");
        return false;
      }
      catch (OverwriteAttemptedException e) {
        System.err.println("SensorData already exists for timestamp=\""
            + data.getTimestamp().toString() + "\". Try overwrite flag?");
      }
      catch (MiscClientException e) {
        System.err.println("Server reported an error (" + e.toString() + ")");
        return false;
      }
      catch (JAXBException e) {
        System.err.println("Unexpected problem creating XML (" + e.toString() + ").");
        return false;
      }
    }
    return true;
  }

  /**
   * Processes command line arguments and starts data input.
   * 
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption("h", "help", false, "print this message");
    options.addOption("f", "file", true, "filename of tabular data");
    options.addOption("w", "uri", true,
        "URI of WattDepot server, ex. \"http://wattdepot.example.com:1234/wattdepot/");
    options
        .addOption("s", "source", true, "Name of the source to send data to, ex. \"foo-source\"");
    options.addOption("u", "username", true, "username to use with server");
    options.addOption("p", "password", true, "password to use with server");
    options.addOption("k", "skip", false, "skip first row of file (column headers)");
    CommandLine cmd = null;
    String filename = null, uri = null, sourceName = null, username = null, password = null;
    boolean skipFirstRow;

    CommandLineParser parser = new PosixParser();
    try {
      cmd = parser.parse(options, args);
    }
    catch (ParseException e) {
      System.err.println("Command line parsing failed. Reason: " + e.getMessage() + ". Exiting.");
      System.exit(1);
    }

    if (cmd.hasOption("h")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      System.exit(0);
    }
    if (cmd.hasOption("f")) {
      filename = cmd.getOptionValue("f");
    }
    else {
      System.err.println("Required filename parameter not provided, exiting.");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    if (cmd.hasOption("w")) {
      uri = cmd.getOptionValue("w");
    }
    else {
      System.err.println("Required URI parameter not provided, exiting.");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    if (cmd.hasOption("s")) {
      sourceName = cmd.getOptionValue("s");
    }
    else {
      System.err.println("Required Source name parameter not provided, exiting.");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    if (cmd.hasOption("u")) {
      username = cmd.getOptionValue("u");
    }
    else {
      System.err.println("Required username parameter not provided, terminating.");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    if (cmd.hasOption("p")) {
      password = cmd.getOptionValue("p");
    }
    else {
      System.err.println("Required password parameter not provided, terminating.");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    skipFirstRow = cmd.hasOption("k");

    // Results of command line processing, should probably be commented out
    System.out.println("filename: " + filename);
    System.out.println("uri:      " + uri);
    System.out.println("source:   " + sourceName);
    System.out.println("username: " + username);
    System.out.println("password: " + password);
    System.out.println("skipFirstLine: " + skipFirstRow);
    // Actually create the input client
    TabularFileSensor inputClient =
        new TabularFileSensor(filename, uri, sourceName, username, password, skipFirstRow);
    // Just do it
    if (inputClient.process()) {
      System.out.println("Successfully input data.");
      System.exit(0);
    }
    else {
      System.err.println("Problem encountered inputting data.");
      System.exit(2);
    }
  }
}
