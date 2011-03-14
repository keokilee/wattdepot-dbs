package org.wattdepot.datainput;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Parses the tabular simulated data format sent by OSCAR into SensorData.
 * 
 * @author Robert Brewer
 */
public class OscarRowParser extends RowParser {

  /** Used to parse dates from the meter data, kept in field to reduce object creation. */
  private SimpleDateFormat format;

  /**
   * Creates the OscarRowParser, and initializes fields based on the provided arguments. Any
   * sourceName provided will be ignored, since OscarRowParser finds the source from the input data.
   * 
   * @param toolName Name of the tool sending the data.
   * @param serverUri URI of WattDepot server to send data to.
   * @param sourceName Should always be null.
   */
  // Is there a better way to do this (prevent a superclass's constructor from being
  // called)?
  private OscarRowParser(String toolName, String serverUri, String sourceName) {
    super(toolName, serverUri, null);
  }

  /**
   * Creates the OscarRowParser, and initializes fields based on the provided arguments.
   * 
   * @param toolName Name of the tool sending the data.
   * @param serverUri URI of WattDepot server to send data to.
   */
  public OscarRowParser(String toolName, String serverUri) {
    super(toolName, serverUri, null);
    // Example date format from OSCAR data: 2009-10-12T00:15:00-1000
    this.format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US);
  }

  /**
   * Converts a row of the table into an appropriate SensorData object.
   * 
   * @param col The row of the table, with each column represented as a String array element.
   * @return The new SensorData object.
   * @throws RowParseException If there are problems parsing the row.
   */
  @Override
  public SensorData parseRow(String[] col) throws RowParseException {
    // Example rows from Oscar:
    // Time, Plant, GridMW, OverheadMW, Type
    // 2009-10-12T00:00:00-1000,SIM_HPOWER,46,5,BASELOAD
    if ((col == null) || (col.length < 5)) {
      // row is missing some columns, so don't try parsing, just give up
      throw new RowParseException("Row missing some columns.");
    }
    String dateString = col[0];
    Date newDate = null;
    try {
      newDate = this.format.parse(dateString);
    }
    catch (java.text.ParseException e) {
      throw new RowParseException("Bad timestamp found in input file: " + dateString, e);
    }
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(newDate.getTime());

    String sourceName = col[1];
    // Create the properties based on the PropertyDictionary
    // http://code.google.com/p/wattdepot/wiki/PropertyDictionary
    String powerGeneratedString = col[2];
    double powerGenerated;
    try {
      // Value in file is a String representing an integer value in MW, while powerGenerated
      // SensorData property is defined in dictionary as being in W, so parse and multiply by 10^6.
      powerGenerated = Integer.parseInt(powerGeneratedString) * 1000 * 1000;
    }
    catch (NumberFormatException e) {
      throw new RowParseException("Unable to parse power generated: " + powerGeneratedString, e);
    }
    return new SensorData(timestamp, this.toolName, Source.sourceToUri(sourceName, this.serverUri),
        new Property(SensorData.POWER_GENERATED, Double.toString(powerGenerated)));
  }
}
