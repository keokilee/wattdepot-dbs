package org.wattdepot.datainput;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Parses the tabular data format sent by Veris meters monitored by the Obvius Acquisuite device.
 * 
 * @author Robert Brewer
 */
public class VerisRowParser extends RowParser {

  /** Used to parse dates from the meter data, kept in field to reduce object creation. */
  private SimpleDateFormat format;

  /**
   * Creates the VerisRowParser, and initializes fields based on the provided arguments.
   * 
   * @param toolName Name of the tool sending the data.
   * @param serverUri URI of WattDepot server to send data to.
   * @param sourceName Name of Source to send data to.
   */
  public VerisRowParser(String toolName, String serverUri, String sourceName) {
    super(toolName, serverUri, sourceName);
    // Example date format from sensor data: 2009-08-01 00:15:12
    this.format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
  }

  /**
   * Converts a row of the table into an appropriate SensorData object.
   * 
   * @param col The row of the table, with each column represented as a String array element.
   * @return The new SensorData object, or null if this row didn't contain useful power data (due to
   * some error on meter side).
   * @throws RowParseException If there are problems parsing the row.
   */
  @Override
  public SensorData parseRow(String[] col) throws RowParseException {
    // Example rows from BMO data (real data is tab separated):
    // time (US/Hawaii) error lowrange highrange Energy Consumption (kWh) Real Power (kW)
    // 2009-08-01 00:00:02 \t 0 \t 0 \t 0 \t 55307.16 \t 3.594
    if ((col == null) || (col.length < 6)) {
      // row is missing some columns, so don't try parsing, just give up
      throw new RowParseException("Row missing some columns.");
    }
    String dateString = col[0];
    Date newDate = null;
    try {
      newDate = format.parse(dateString);
    }
    catch (java.text.ParseException e) {
      throw new RowParseException("Bad timestamp found in input file: " + dateString, e);
    }
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(newDate.getTime());

    // Apparently the error column indicates if something went wrong, so if the value is != 0
    // the rest of the columns for this row should be ignored.
    if ("0".equals(col[1])) {
      // // DEBUG
      // DateFormat df = DateFormat.getDateTimeInstance();
      // System.out.println("Input date: " + dateString + ", output date: " + df.format(newDate));

      // Create the properties based on the PropertyDictionary
      // http://code.google.com/p/wattdepot/wiki/PropertyDictionary
      String powerConsumedString = col[5];
      double powerConsumed;
      try {
        // Value in file is a String representing a floating point value in kW, while powerConsumed
        // SensorData property is defined in dictionary as being in W, so parse and multiply by
        // 1000.
        powerConsumed = Double.parseDouble(powerConsumedString) * 1000;
      }
      catch (NumberFormatException e) {
        throw new RowParseException(
            "Unable to parse floating point number: " + powerConsumedString, e);
      }
      Property prop1 = new Property(SensorData.POWER_CONSUMED, Double.toString(powerConsumed));

      String energyConsumedToDateString = col[4];
      double energyConsumedToDate;
      try {
        // Value in file is a String representing a floating point value in kWh, while
        // energyConsumedToDate SensorData property is defined in dictionary as being in Wh, so
        // parse
        // and multiply by 1000.
        energyConsumedToDate = Double.parseDouble(energyConsumedToDateString) * 1000;
      }
      catch (NumberFormatException e) {
        throw new RowParseException("Unable to parse floating point number: "
            + energyConsumedToDateString, e);
      }
      Property prop2 =
          new Property(SensorData.ENERGY_CONSUMED_TO_DATE, Double.toString(energyConsumedToDate));
      Properties props = new Properties();
      props.getProperty().add(prop1);
      props.getProperty().add(prop2);

      return new SensorData(timestamp, this.toolName, Source.sourceToUri(this.sourceName,
          this.serverUri), props);
    }
    else {
      // Row has no data due to error, so return null
      return null;
    }
  }
}
