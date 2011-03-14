package org.wattdepot.datainput;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDatas;
import au.com.bytecode.opencsv.CSVReader;

/**
 * Reads the table file produced by <a href="http://code.google.com/p/oscar-project/">OSCAR</a> and
 * turns it into a series of files, each containing a SensorData object in XML format, one per row
 * of the input file.
 * 
 * @author Robert Brewer
 */
public class OscarDataConverter {

  /** Name of the application on the command line. */
  protected static final String TOOL_NAME = "OscarDataConverter";

  /** Name of the file to be input. */
  protected String filename;

  /** Name of the directory where sensordata will be output. */
  protected String directory;

  /** URI of WattDepot server this data will live in. */
  protected String serverUri;

  /** Whether to write output to a single big file or not. */
  protected boolean singleFile;

  /** The parser used to turn rows into SensorData objects. */
  protected RowParser parser;

  /**
   * Creates the new OscarDataConverter.
   * 
   * @param filename Name of the file to read data from.
   * @param directory Name of the directory where sensordata will be output.
   * @param uri URI of the server to send data to (ending in "/").
   * @param singleFile True if output is to be written to a single file.
   */
  public OscarDataConverter(String filename, String directory, String uri, boolean singleFile) {
    this.filename = filename;
    this.directory = directory;
    this.serverUri = uri;
    this.singleFile = singleFile;
    this.parser = new OscarRowParser(TOOL_NAME, this.serverUri);
  }

  /**
   * Does the work of reading the Oscar data and converting it into SensorData, one row at a time.
   * 
   * @return True if the data could be successfully input, false otherwise.
   * @throws IOException If problems are encountered reading or writing to files.
   */
  public boolean process() throws IOException {
    // JAXB setup
    JAXBContext sensorDataJAXB;
    Marshaller marshaller;
    try {
      sensorDataJAXB =
          JAXBContext.newInstance(org.wattdepot.resource.sensordata.jaxb.ObjectFactory.class);
      marshaller = sensorDataJAXB.createMarshaller();
    }
    catch (JAXBException e) {
      System.err.format("Problem creating JAXBContext: %s%n", e);
      return false;
    }

    // Check that our input file is available
    File inputFile = new File(this.filename), outputDir = new File(this.directory);
    CSVReader reader;
    try {
      // use 4 arg constructor with skip lines = 1 to skip the column header
      reader = new CSVReader(new FileReader(filename), ',', CSVReader.DEFAULT_QUOTE_CHARACTER, 1);
    }
    catch (FileNotFoundException e) {
      System.err.format("Input file %s not found.%n", inputFile.toString());
      return false;
    }
    // Check that output directory is available
    if (!outputDir.isDirectory()) {
      System.err.format("Output directory %s not found.%n", outputDir.toString());
      return false;
    }

    String[] nextLine;
    File outputFile;
    int rowsConverted = 0;
    SensorDatas datas = new SensorDatas();

    while ((nextLine = reader.readNext()) != null) {
      // nextLine[] is an array of values from the current row in the file
      SensorData data = null;
      try {
        data = this.parser.parseRow(nextLine);
      }
      catch (RowParseException e) {
        System.err.println(e);
      }
      if (data != null) {
        if (singleFile) {
          // Just append to the list, which will be written out later
          datas.getSensorData().add(data);
        }
        else {
          // Create a new output file in the output directory named by source name and timestamp
          String sourceUri = data.getSource();
          String sourceName = sourceUri.substring(sourceUri.lastIndexOf('/') + 1);
          outputFile = new File(outputDir, sourceName + "_" + data.getTimestamp().toString());
          try {
            marshaller.marshal(data, outputFile);
          }
          catch (JAXBException e) {
            System.err.format("Problem creating writing output file: %s %s%n", outputFile, e);
            return false;
          }
        }
        rowsConverted++;
      }
    }
    reader.close();
    // If we are in singleFile mode and there is output data
    if (singleFile && datas.isSetSensorData()) {
      outputFile = new File(outputDir, "sensordata.xml");
      System.out.format("Writing out %d sensordata entries in one big file...%n", rowsConverted);
      try {
        marshaller.marshal(datas, outputFile);
      }
      catch (JAXBException e) {
        System.err.format("Problem creating writing output file: %s %s%n", outputFile, e);
        return false;
      }
    }
    System.out.format("Converted %d rows of input data.%n", rowsConverted);
    return true;
  }

  /**
   * Processes command line arguments and starts data input.
   * 
   * @param args command line arguments.
   * @throws IOException If there are problems reading the input or output files.
   */
  public static void main(String[] args) throws IOException {
    Options options = new Options();
    options.addOption("h", "help", false, "print this message");
    options.addOption("f", "file", true, "filename of tabular data");
    options.addOption("d", "directory", true, "directory where sensordata files will be written");
    options.addOption("w", "uri", true,
        "URI of WattDepot server, ex. \"http://wattdepot.example.com:1234/wattdepot/\"."
            + " Note that this parameter is used only to populate the SensorData object "
            + "Source field, no network connection will be made.");
    options.addOption("s", "singleFile", false, "output will be written to one big file");
    CommandLine cmd = null;
    String filename = null, directory = null, uri = null;
    boolean singleFile = false;

    CommandLineParser parser = new PosixParser();
    try {
      cmd = parser.parse(options, args);
    }
    catch (ParseException e) {
      System.err.println("Command line parsing failed. Reason: " + e.getMessage() + ". Exiting.");
      System.exit(1);
    }

    HelpFormatter formatter = new HelpFormatter();
    if (cmd.hasOption("h")) {
      formatter.printHelp(TOOL_NAME, options);
      System.exit(0);
    }
    if (cmd.hasOption("s")) {
      singleFile = true;
    }

    // required options
    if (cmd.hasOption("f")) {
      filename = cmd.getOptionValue("f");
    }
    else {
      System.err.println("Required filename parameter not provided, exiting.");
      formatter.printHelp(TOOL_NAME, options);
      System.exit(1);
    }
    if (cmd.hasOption("d")) {
      directory = cmd.getOptionValue("d");
    }
    else {
      System.err.println("Required directory parameter not provided, exiting.");
      formatter.printHelp(TOOL_NAME, options);
      System.exit(1);
    }
    if (cmd.hasOption("w")) {
      uri = cmd.getOptionValue("w");
    }
    else {
      System.err.println("Required URI parameter not provided, exiting.");
      formatter.printHelp(TOOL_NAME, options);
      System.exit(1);
    }

    // Results of command line processing, should probably be commented out
    System.out.println("filename:   " + filename);
    System.out.println("directory:  " + directory);
    System.out.println("uri:        " + uri);
    System.out.println("singleFile: " + singleFile);
    // Actually create the input client
    OscarDataConverter inputClient = new OscarDataConverter(filename, directory, uri, singleFile);
    // Just do it
    if (inputClient.process()) {
      System.out.println("Successfully converted data.");
      System.exit(0);
    }
    else {
      System.err.println("Problem encountered converting data.");
      System.exit(2);
    }
  }
}
