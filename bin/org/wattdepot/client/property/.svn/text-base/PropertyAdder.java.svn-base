package org.wattdepot.client.property;

import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.IOException;
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
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.logger.WattDepotUserHome;

/**
 * Adds the supportsEnergyCounters property to any Source that contains sensor data that contains
 * energy counter properties.
 * 
 * @author Robert Brewer
 */
public class PropertyAdder {
  /** The URI of the server to be updated. */
  private String uri;

  /** Name of the property file containing essential preferences. */
  protected DataInputClientProperties properties;

  /** Name of this tool. */
  private static final String toolName = "PropertyAdder";

  /** Making PMD happy. */
  private static final String REQUIRED_PARAMETER_ERROR_MSG =
      "Required parameter %s not found in properties.%n";

  /** If true, just print out what would happen without making changes to Sources. */
  private boolean dryRun;

  /**
   * Creates the new PropertyAdder client.
   * 
   * @param propertyFilename Name of the file to read essential properties from. Defaults to
   * ~/.wattdepot/client/datainput.properties
   * @param uri The URI of the WattDepot server.
   * @param dryRun If true, just print out what would happen without making changes to Sources.
   * @throws IOException If the property file cannot be found.
   */
  public PropertyAdder(String propertyFilename, String uri, boolean dryRun) throws IOException {
    if (propertyFilename == null) {
      // Use default property file name
      this.properties =
          new DataInputClientProperties(WattDepotUserHome.getHomeString()
              + "/.wattdepot/client/datainput.properties");
    }
    else {
      this.properties = new DataInputClientProperties(propertyFilename);
    }

    this.uri = uri;
    this.dryRun = dryRun;
  }

  /**
   * Actually adds the property as appropriate.
   * 
   * @return True if all sources were processed without incident, false if any problems were
   * encountered.
   */
  public boolean process() {
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
    WattDepotClient client = new WattDepotClient(this.uri, wattDepotUsername, wattDepotPassword);
    List<Source> sources;
    if (client.isHealthy()) {
      try {
        sources = client.getSources();
      }
      catch (NotAuthorizedException e) {
        System.err.format("Bad credentials in properties file.%n");
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

      SensorData data;
      // Iterate through all sources
      for (Source source : sources) {
        if (source.isVirtual()) {
          // we only care about non-virtual sources, so skip
          System.out.format("Skipping virtual source %s%n", source.getName());
          continue;
        }
        String counterSupport = source.getProperty(Source.SUPPORTS_ENERGY_COUNTERS);
        if (counterSupport == null) {
          // counter support unknown, so see if latest sensor data contains counters
          try {
            data = client.getLatestSensorData(source.getName());
          }
          catch (NotAuthorizedException e) {
            // Any lack of authorization should have been caught when fetching source, but whatever
            System.err.format("Given credentials do not allow sensor data retrieval. Aborting.%n");
            return false;
          }
          catch (ResourceNotFoundException e) {
            System.err.format("Source %s does not have any sensor data, skipping.%n", source
                .getName());
            continue;
          }
          catch (BadXmlException e) {
            System.err.println("Received bad XML from server, which is weird. Aborting.");
            return false;
          }
          catch (MiscClientException e) {
            System.err
                .println("Had problems retrieving sensor data from server, which is weird. Aborting.");
            return false;
          }
                    
          if ((data.getProperty(SensorData.ENERGY_CONSUMED_TO_DATE) == null)
              && (data.getProperty(SensorData.ENERGY_GENERATED_TO_DATE) == null)) {
            System.out.format(
                "Latest sensor data from source %s doesn't have counters, skipping %n", source
                    .getName());
          }
          else {
            // Found counter(s) in latest sensor data, so add source property
            if (dryRun) {
              System.out.format(
                  "dryRun: would have added energy counter properties to source %s%n", source
                      .getName());
            }
            else {
              source.addProperty(new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true"));
              try {
                if (!client.storeSource(source, true)) {
                  System.err.format(
                      "Had problems updating Source %s on server, which is weird. Aborting.",
                      source.getName());
                  return false;
                }
                System.out.format("Added energy counter properties to source %s%n", source
                    .getName());
              }
              catch (NotAuthorizedException e) {
                System.err.format(
                    "Had problems updating Source %s on server, which is weird. Aborting.", source
                        .getName());
              }
              catch (BadXmlException e) {
                System.err.println("Received bad XML from server, which is weird. Aborting.");
                return false;
              }
              catch (OverwriteAttemptedException e) {
                System.err
                    .println("Attempted to overwrite source without overwrite flag (should never "
                        + "happen). Aborting.");
                return false;
              }
              catch (MiscClientException e) {
                System.err
                    .println("Had problems storing Source on server, which is weird. Aborting.");
                return false;
              }
              catch (JAXBException e) {
                System.err.println("Had some problem marshalling XML , which is weird. Aborting.");
                return false;
              }
            }
          }
        }
        else {
          // Source already explicitly specifies value for counter support, so skip it
          System.out.format("Source %s already has counter support set to \"%s\", skipping%n",
              source.getName(), counterSupport);

          continue;
        }
      }
      return true;
    }
    else {
      System.err.println("Unable to connect to WattDepot server. Aborting.");
      return false;
    }
  }

  /**
   * Processes command line arguments, creates the MonitorSourceClient object and starts monitoring.
   * 
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("u", "uri", true,
        "URI of WattDepot server, ex. \"http://server.wattdepot.org:8182/wattdepot/\".");
    options.addOption("p", "propertyFilename", true, "Filename of property file");
    options
        .addOption("n", "dryRun", false, "Just print what would happen without changing Sources");

    CommandLine cmd = null;
    String uri = null, propertyFilename = null;
    boolean dryRun;

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

    if (cmd.hasOption("p")) {
      propertyFilename = cmd.getOptionValue("p");
    }
    dryRun = cmd.hasOption("n");

    // Results of command line processing, should probably be commented out
    System.out.println("uri: " + uri);
    System.out.println("propertyFilename: " + propertyFilename);
    System.out.println("dryRun: " + dryRun);
    System.out.println();

    // Actually create the input client
    PropertyAdder propertyAdder = null;
    try {
      propertyAdder = new PropertyAdder(propertyFilename, uri, dryRun);
    }
    catch (IOException e) {
      System.err.println("Unable to read properties file, terminating.");
      System.exit(2);
    }
    // Just do it
    if ((propertyAdder != null) && (propertyAdder.process())) {
      System.out.println("Properties added successfully.");
      System.exit(0);
    }
    else {
      System.err.println("Error encountered while trying to add properties.");
      System.exit(2);
    }
  }
}