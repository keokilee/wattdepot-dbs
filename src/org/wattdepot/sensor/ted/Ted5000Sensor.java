package org.wattdepot.sensor.ted;

import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.w3c.dom.Document;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.MiscClientException;
import org.wattdepot.client.NotAuthorizedException;
import org.wattdepot.client.ResourceNotFoundException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;
import org.xml.sax.SAXException;

/**
 * Polls a TED 5000 home energy monitor for power data periodically, and sends the results to a
 * WattDepot server. For more information about the XML that the TED generates, see
 * http://code.google.com/p/wattdepot/wiki/Ted5000XmlExplanation
 * 
 * @author Robert Brewer
 */
public class Ted5000Sensor {

  /** Name of the property file containing essential preferences. */
  protected DataInputClientProperties properties;
  /** The name of the source to be monitored. */
  private String sourceName;
  /** The rate at which to poll the source for new data. */
  private int updateRate;
  /** Whether to display debugging data. */
  private boolean debug;
  /** The hostname of the TED 5000 to be monitored. */
  private String tedHostname;

  /** Name of this tool. */
  private static final String toolName = "Ted5000Sensor";
  /** The default polling rate, in seconds. */
  private static final int DEFAULT_UPDATE_RATE = 10;
  /** The polling rate that indicates that it needs to be set to a default. */
  private static final int UPDATE_RATE_SENTINEL = 0;

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
   * @param tedHostname The hostname of the TED 5000 sensor to be polled.
   * @throws IOException If the property file cannot be found or read.
   */
  public Ted5000Sensor(String propertyFilename, String sourceName, int updateRate, boolean debug,
      String tedHostname) throws IOException {
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
    this.tedHostname = tedHostname;
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

    String sourceURI = Source.sourceToUri(this.sourceName, wattDepotURI);

    WattDepotClient client =
        new WattDepotClient(wattDepotURI, wattDepotUsername, wattDepotPassword);
    Source source;
    if (client.isAuthenticated()) {
      try {
        source = client.getSource(this.sourceName);
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
      while (true) {
        // Get data from TED
        try {
          data = pollTed(this.tedHostname, toolName, sourceURI);
        }
        catch (XPathExpressionException e) {
          System.err.println("Bad XPath expression, this should never happen.");
          return false;
        }
        catch (ParserConfigurationException e) {
          System.err.println("Unable to configure XML parser, this is weird.");
          return false;
        }
        catch (SAXException e) {
          System.err.format("%s: Got bad XML from TED meter (%s), hopefully this is temporary.%n",
              Tstamp.makeTimestamp(), e);
          Thread.sleep(updateRate * 1000);
          continue;
        }
        catch (IOException e) {
          System.err.format(
              "%s: Unable to retrieve data from TED (%s), hopefully this is temporary.%n", Tstamp
                  .makeTimestamp(), e);
          Thread.sleep(updateRate * 1000);
          continue;
        }
        // Store SensorData in WattDepot server
        try {
          client.storeSensorData(data);
        }
        catch (Exception e) {
          System.err
              .format(
                  "%s: Unable to store sensor data due to exception (%s), hopefully this is temporary.%n",
                  Tstamp.makeTimestamp(), e);
        }
        if (debug) {
          System.out.println(data);
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
   * Connects to a TED 5000 meter gateway, retrieves the latest data, and returns it as a SensorData
   * object.
   * 
   * @param hostname The hostname of the TED 5000. Only the hostname, not URI.
   * @param toolName The name of the tool to be placed in the SensorData.
   * @param sourceURI The URI of the source (needed to create SensorData object).
   * @return The meter data as SensorData
   * @throws ParserConfigurationException If there are problems creating a parser.
   * @throws IOException If there are problems retrieving the data via HTTP.
   * @throws SAXException If there are problems parsing the XML from the meter.
   * @throws XPathExpressionException If the hardcoded XPath expression is bogus.
   * @throws IllegalArgumentException If the provided hostname is invalid.
   * 
   */
  private SensorData pollTed(String hostname, String toolName, String sourceURI)
      throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
    String tedURI = "http://" + hostname + "/api/LiveData.xml";

    DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
    domFactory.setNamespaceAware(true);
    DocumentBuilder builder = domFactory.newDocumentBuilder();

    if ((hostname == null) || (hostname.length() == 0)) {
      throw new IllegalArgumentException("No hostname was provided");
    }
    
    // Have to make HTTP connection manually so we can set proper timeouts
    URL url;
    try {
      url = new URL(tedURI);
    }
    catch (MalformedURLException e) {
      throw new IllegalArgumentException("Hostname was invalid leading to malformed URL", e);
    }
    URLConnection httpConnection;
    httpConnection = url.openConnection();
    // Set both connect and read timeouts to 15 seconds. No point in long timeouts since the
    // sensor will retry before too long anyway.
    httpConnection.setConnectTimeout(15 * 1000);
    httpConnection.setReadTimeout(15 * 1000);
    httpConnection.connect();

    // Record current time as close approximation to time for reading we are about to make
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp();

    Document doc = builder.parse(httpConnection.getInputStream());

    XPathFactory factory = XPathFactory.newInstance();
    XPath powerXpath = factory.newXPath();
    XPath energyXpath = factory.newXPath();
    // Path to get the current power consumed measured by the meter in watts
    XPathExpression exprPower = powerXpath.compile("/LiveData/Power/Total/PowerNow/text()");
    // Path to get the energy consumed month to date in watt hours
    XPathExpression exprEnergy = energyXpath.compile("/LiveData/Power/Total/PowerMTD/text()");
    Object powerResult = exprPower.evaluate(doc, XPathConstants.NUMBER);
    Object energyResult = exprEnergy.evaluate(doc, XPathConstants.NUMBER);

    Double currentPower = (Double) powerResult;
    Double mtdEnergy = (Double) energyResult;

    SensorData data = new SensorData(timestamp, toolName, sourceURI);
    data.addProperty(new Property(SensorData.POWER_CONSUMED, currentPower));
    data.addProperty(new Property(SensorData.ENERGY_CONSUMED_TO_DATE, mtdEnergy));
    return data;
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
        "The rate at which to query the meter, in seconds. If not specified, will "
            + "default to value of source's updateInterval property, or " + DEFAULT_UPDATE_RATE
            + " seconds if source has no such property");
    options.addOption("t", "tedHostname", true,
        "Hostname of TED 5000 gateway. Note just hostname, not full URI");
    options.addOption("d", "debug", false, "Displays sensor data as it is sent to the server.");

    CommandLine cmd = null;
    String propertyFilename = null, sourceName = null, tedHostname = null;
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
    if (cmd.hasOption("t")) {
      tedHostname = cmd.getOptionValue("t");
    }
    else {
      System.err.println("Required tedHostname parameter not provided, exiting.");
      formatter.printHelp(toolName, options);
      System.exit(1);
    }
    debug = cmd.hasOption("d");

    if (debug) {
      System.out.println("propertyFilename: " + propertyFilename);
      System.out.println("sourceName: " + sourceName);
      System.out.println("updateRate: " + updateRate);
      System.out.println("tedHostname: " + tedHostname);
      System.out.println("debug: " + debug);
      System.out.println();
    }

    // Actually create the input client
    Ted5000Sensor sensor = null;
    try {
      sensor = new Ted5000Sensor(propertyFilename, sourceName, updateRate, debug, tedHostname);
    }
    catch (IOException e) {
      System.err.println("Unable to read properties file, terminating.");
      System.exit(2);
    }
    // Just do it
    if (sensor != null) {
      System.err.format("Starting polling TED at %s%n", Tstamp.makeTimestamp());
      sensor.process();
    }
  }
}
