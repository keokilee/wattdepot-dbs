package org.wattdepot.sensor.modbus;

import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import javax.xml.datatype.XMLGregorianCalendar;
import net.wimpi.modbus.Modbus;
import net.wimpi.modbus.io.ModbusTCPTransaction;
import net.wimpi.modbus.msg.ExceptionResponse;
import net.wimpi.modbus.msg.ModbusResponse;
import net.wimpi.modbus.msg.ReadMultipleRegistersRequest;
import net.wimpi.modbus.msg.ReadMultipleRegistersResponse;
import net.wimpi.modbus.net.TCPMasterConnection;
import net.wimpi.modbus.util.ModbusUtil;
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
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Polls a ElectroIndustries Shark power meter for power & energy data periodically, and sends the
 * results to a WattDepot server. For more information about Shark meters, see:
 * http://www.electroind.com/pdf/Shark_New/E149721_Shark_200-S_Meter_Manual_V.1.03.pdf
 * 
 * Inspiration for this sensor came from the work on the WattDepot Modbus sensor:
 * http://code.google.com/p/wattdepot-sensor-modbus/
 * 
 * @author Robert Brewer
 */
public class SharkSensor {

  /** Name of the property file containing essential preferences. */
  protected DataInputClientProperties properties;
  /** The name of the source to send data to. */
  private String sourceName;
  /** The rate at which to poll the device for new data. */
  private int updateRate;
  /** Whether to display debugging data. */
  private boolean debug;
  /** The hostname of the Shark meter to be monitored. */
  private String meterHostname;

  // Note that the Modbus register map in the Shark manual appears to start at 1, while jamod
  // expects it to start at 0, so you must subtract 1 from the register index listed in the manual!
  /** Register index for "Power & Energy Format". */
  private static final int ENERGY_FORMAT_REGISTER = 30006 - 1;
  /** Number of words (registers) that make up "Power & Energy Format". */
  private static final int ENERGY_FORMAT_LENGTH = 1;
  /** Register index for "W-hours, Total". */
  private static final int ENERGY_REGISTER = 1506 - 1;
  /** Number of words (registers) that make up "W-hours, Total". */
  private static final int ENERGY_LENGTH = 2;
  /** Register index for "Watts, 3-Ph total". */
  private static final int POWER_REGISTER = 1018 - 1;
  /** Number of words (registers) that make up "Watts, 3-Ph total". */
  private static final int POWER_LENGTH = 2;

  /** Name of this tool. */
  private static final String toolName = "Shark200SSensor";
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
   * @param sourceName The name of the source to send data to.
   * @param updateRate The rate at which to send new data to the source, in seconds.
   * @param debug If true then display new sensor data when sending it.
   * @param meterHostname The hostname of the meter to be polled.
   * @throws IOException If the property file cannot be found or read.
   */
  public SharkSensor(String propertyFilename, String sourceName, int updateRate, boolean debug,
      String meterHostname) throws IOException {
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
    this.meterHostname = meterHostname;
  }

  /**
   * Retrieves meter sensor data periodically and sends it to a Source in a WattDepot server.
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

      // Resolve provided meter hostname
      InetAddress meterAddress = null;
      try {
        meterAddress = InetAddress.getByName(meterHostname);
      }
      catch (UnknownHostException e) {
        System.err.println("Unable to resolve provided meter hostname, aborting.");
        return false;
      }

      // Need to retrieve some configuration parameters from meter, but only want to do it once
      // per session.
      ModbusResponse response;
      ReadMultipleRegistersResponse goodResponse = null;
      try {
        response = readRegisters(meterAddress, ENERGY_FORMAT_REGISTER, ENERGY_FORMAT_LENGTH);
      }
      catch (Exception e) {
        System.err.format(
            "Unable to retrieve energy format parameters from meter: %s, aborting.%n", e
                .getMessage());
        return false;
      }
      if (response instanceof ReadMultipleRegistersResponse) {
        goodResponse = (ReadMultipleRegistersResponse) response;
      }
      else if (response instanceof ExceptionResponse) {
        System.err
            .println("Got Modbus exception response while retrieving energy format parameters from meter, code: "
                + ((ExceptionResponse) response).getExceptionCode());
        return false;
      }
      else {
        System.err
            .println("Got strange Modbus reply while retrieving energy format parameters from meter, aborting.");
        return false;
      }

      double energyMultiplier = decodeEnergyMultiplier(goodResponse);
      if (energyMultiplier == 0) {
        System.err.println("Got bad energy multiplier from meter energy format, aborting.");
        return false;
      }

      int energyDecimals = decodeEnergyDecimals(goodResponse);
      if (energyDecimals == 0) {
        System.err.println("Got bad energy decimal format from meter energy format, aborting.");
        return false;
      }

      while (true) {
        // Get data from meter
        data = pollMeter(meterAddress, toolName, sourceURI, energyMultiplier, energyDecimals);
        if (data == null) {
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
      System.err.format("Invalid credentials for source %s. Aborting.%n", this.sourceName);
      return false;
    }
  }

  /**
   * Reads one or more consecutive registers from a device using Modbus/TCP using the default TCP
   * port (502).
   * 
   * @param address The IP address of the device to be read from
   * @param register The Modbus register to be read, as a decimal integer
   * @param length The number of registers to read
   * @return A ReadMultipleRegistersResponse containing the response from the device, or null if
   * there was some unexpected error.
   * @throws Exception If an error occurs while attempting to read the registers.
   */
  private ModbusResponse readRegisters(InetAddress address, int register, int length)
      throws Exception {
    return readRegisters(address, Modbus.DEFAULT_PORT, register, length);
  }

  /**
   * Reads one or more consecutive registers from a device using Modbus/TCP.
   * 
   * @param address The IP address of the device to be read from
   * @param port The destination TCP port to connect to
   * @param register The Modbus register to be read, as a decimal integer
   * @param length The number of registers to read
   * @return A ReadMultipleRegistersResponse containing the response from the device, or null if
   * there was some unexpected error.
   * @throws Exception If an error occurs while attempting to read the registers.
   */
  private ModbusResponse readRegisters(InetAddress address, int port, int register, int length)
      throws Exception {
    TCPMasterConnection connection = null;
    ModbusTCPTransaction transaction = null;
    ReadMultipleRegistersRequest request = null;

    // Open the connection
    connection = new TCPMasterConnection(address);
    connection.setPort(port);
    connection.connect();

    // Prepare the request
    request = new ReadMultipleRegistersRequest(register, length);

    // Prepare the transaction
    transaction = new ModbusTCPTransaction(connection);
    transaction.setRequest(request);
    transaction.execute();
    ModbusResponse response = transaction.getResponse();

    // Close the connection
    connection.close();

    return response;
  }

  /**
   * Decodes the energy multiplier configured on the meter, which really means whether the energy
   * value returned is in Wh, kWh, or MWh.
   * 
   * @param response The response from the meter containing the power and energy format.
   * @return A double that represents the scale which energy readings should be multiplied by, or 0
   * if there was some problem decoding the value.
   */
  private double decodeEnergyMultiplier(ReadMultipleRegistersResponse response) {
    if ((response != null) && (response.getWordCount() == 1)) {
      // From Shark manual, bitmap looks like this ("-" is unused bit apparently):
      // ppppiinn feee-ddd
      //
      // pppp = power scale (0-unit, 3-kilo, 6-mega, 8-auto)
      // ii = power digits after decimal point (0-3),
      // applies only if f=1 and pppp is not auto
      // nn = number of energy digits (5-8 --> 0-3)
      // eee = energy scale (0-unit, 3-kilo, 6-mega)
      // f = decimal point for power
      // (0=data-dependant placement, 1=fixed placement per ii value)
      // ddd = energy digits after decimal point (0-6)

      // Get energy scale by shifting off 4 bits and then mask with 111
      int energyScale = (response.getRegisterValue(0) >>> 4) & 7;
      switch (energyScale) {
      case 0:
        // watts
        return 1.0;
      case 3:
        // kilowatts
        return 1000.0;
      case 6:
        // megawatts
        return 1000000.0;
      default:
        // should never happen, according to manual, so return 0
        // System.err.println("Unknown energy scale from meter, defaulting to kWh");
        return 0.0;
      }
    }
    else {
      return 0.0;
    }
  }

  /**
   * Decodes the configured number of energy digits after the decimal point. For example, if the
   * retrieved energy value is "12345678" and the decimals value is 2, then the actual energy value
   * is "123456.78".
   * 
   * @param response The response from the meter containing the power and energy format.
   * @return An int that represents the number of ending digits from the energy reading that should
   * be considered as decimals.
   */
  private int decodeEnergyDecimals(ReadMultipleRegistersResponse response) {
    if ((response != null) && (response.getWordCount() == 1)) {
      // From Shark manual, bitmap looks like this ("-" is unused bit apparently):
      // ppppiinn feee-ddd
      //
      // pppp = power scale (0-unit, 3-kilo, 6-mega, 8-auto)
      // ii = power digits after decimal point (0-3),
      // applies only if f=1 and pppp is not auto
      // nn = number of energy digits (5-8 --> 0-3)
      // eee = energy scale (0-unit, 3-kilo, 6-mega)
      // f = decimal point for power
      // (0=data-dependant placement, 1=fixed placement per ii value)
      // ddd = energy digits after decimal point (0-6)

      // Get # of energy digits after decimal point by masking with 111
      return response.getRegisterValue(0) & 7;
    }
    else {
      return 0;
    }
  }

  /**
   * Given a response with two consecutive registers, extract the values as a 4 byte array so they
   * can be passed to methods in ModbusUtil. It seems like there should be a better way to do this.
   * 
   * @param response The response containing the two registers
   * @return a byte[4] array or null if there is a problem with the response.
   */
  private byte[] extractByteArray(ReadMultipleRegistersResponse response) {
    byte[] regBytes = new byte[4];
    if (response.getWordCount() == 2) {
      regBytes[0] = response.getRegister(0).toBytes()[0];
      regBytes[1] = response.getRegister(0).toBytes()[1];
      regBytes[2] = response.getRegister(1).toBytes()[0];
      regBytes[3] = response.getRegister(1).toBytes()[1];
      return regBytes;
    }
    else {
      return null;
    }
  }

  /**
   * Connects to meter, retrieves the latest data, and returns it as a SensorData object.
   * 
   * @param meterAddress The address of the meter.
   * @param toolName The name of the tool to be placed in the SensorData.
   * @param sourceURI The URI of the source (needed to create SensorData object).
   * @param energyMultiplier The amount to multiply energy readings by to account for unit of
   * measurement.
   * @param energyDecimals The configured number of decimals included in the energy reading.
   * @return The meter data as SensorData
   */
  private SensorData pollMeter(InetAddress meterAddress, String toolName, String sourceURI,
      double energyMultiplier, int energyDecimals) {
    return pollMeter(meterAddress, Modbus.DEFAULT_PORT, toolName, sourceURI, energyMultiplier,
        energyDecimals);
  }

  /**
   * Connects to meter, retrieves the latest data, and returns it as a SensorData object.
   * 
   * @param meterAddress The address of the meter.
   * @param port The destination TCP port to connect to
   * @param toolName The name of the tool to be placed in the SensorData.
   * @param sourceURI The URI of the source (needed to create SensorData object).
   * @param energyMultiplier The amount to multiply energy readings by to account for unit of
   * measurement.
   * @param energyDecimals The configured number of decimals included in the energy reading.
   * @return The meter data as SensorData
   */
  private SensorData pollMeter(InetAddress meterAddress, int port, String toolName,
      String sourceURI, double energyMultiplier, int energyDecimals) {

    // Record current time as close approximation to time for reading we are about to make
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp();
    ModbusResponse energyResponse, powerResponse;
    ReadMultipleRegistersResponse goodResponse = null;
    try {
      // Make both queries in rapid succession to reduce any lag on sensor side
      energyResponse = readRegisters(meterAddress, port, ENERGY_REGISTER, ENERGY_LENGTH);
      powerResponse = readRegisters(meterAddress, port, POWER_REGISTER, POWER_LENGTH);
    }
    catch (Exception e) {
      System.err.format("%s Unable to retrieve energy data from meter: %s.%n", new Date(), e
          .getMessage());
      return null;
    }

    // First handle energy response
    if (energyResponse instanceof ReadMultipleRegistersResponse) {
      goodResponse = (ReadMultipleRegistersResponse) energyResponse;
    }
    else if (energyResponse instanceof ExceptionResponse) {
      System.err
          .println("Got Modbus exception response while retrieving energy data from meter, code: "
              + ((ExceptionResponse) energyResponse).getExceptionCode());
      return null;
    }
    else {
      System.err.println("Got strange Modbus reply while retrieving energy data from meter.");
      return null;
    }
    int wattHoursInt = ModbusUtil.registersToInt(extractByteArray(goodResponse));
    // Take integer value, divide by 10^energyDecimals to move decimal point to right place,
    // then multiply by a value depending on units (nothing, kilo, or mega).
    double wattHours = (wattHoursInt / (Math.pow(10.0, energyDecimals))) * energyMultiplier;

    // Then handle power response
    if (powerResponse instanceof ReadMultipleRegistersResponse) {
      goodResponse = (ReadMultipleRegistersResponse) powerResponse;
    }
    else if (powerResponse instanceof ExceptionResponse) {
      System.err
          .println("Got Modbus exception response while retrieving power data from meter, code: "
              + ((ExceptionResponse) powerResponse).getExceptionCode());
      return null;
    }
    else {
      System.err.println("Got strange Modbus reply while retrieving power data from meter.");
      return null;
    }
    float watts = ModbusUtil.registersToFloat(extractByteArray(goodResponse));

    SensorData data = new SensorData(timestamp, toolName, sourceURI);
    data.addProperty(new Property(SensorData.POWER_CONSUMED, watts));
    data.addProperty(new Property(SensorData.ENERGY_CONSUMED_TO_DATE, wattHours));
    return data;
  }

  /**
   * Processes command line arguments, creates the SharkSensor object and starts sending.
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
    options.addOption("m", "meterHostname", true,
        "Hostname of meter. Note just hostname (or IP address), not full URI");
    options.addOption("d", "debug", false, "Displays sensor data as it is sent to the server.");

    CommandLine cmd = null;
    String propertyFilename = null, sourceName = null, meterHostname = null;
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
    if (cmd.hasOption("m")) {
      meterHostname = cmd.getOptionValue("m");
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
      System.out.println("meterHostname: " + meterHostname);
      System.out.println("debug: " + debug);
      System.out.println();
    }

    // Actually create the input client
    SharkSensor sensor = null;
    try {
      sensor = new SharkSensor(propertyFilename, sourceName, updateRate, debug, meterHostname);
    }
    catch (IOException e) {
      System.err.println("Unable to read properties file, terminating.");
      System.exit(2);
    }
    // Just do it
    if (sensor != null) {
      System.err.format("Starting polling meter at %s%n", Tstamp.makeTimestamp());
      sensor.process();
    }
  }
}
