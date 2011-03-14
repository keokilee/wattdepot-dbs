package org.wattdepot.datainput;

import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * Specifies the interface for parsing a row of tabular data into a SensorData object. An
 * implementation of this interface can be used in classes that need to read tabular data from a
 * file or a network stream.
 * 
 * @author Robert Brewer
 */
public abstract class RowParser {

  /** Name of the tool sending the data. Needed to build a SensorData object.*/
  protected String toolName;

  /** URI of WattDepot server to send data to. Needed to build a SensorData object. */
  protected String serverUri;

  /** Name of Source to send data to. Needed to build a SensorData object. */
  protected String sourceName;

  /**
   * Creates the RowParser, and initializes fields based on the provided arguments.
   * 
   * @param toolName Name of the tool sending the data.
   * @param serverUri URI of WattDepot server to send data to.
   * @param sourceName Name of Source to send data to.
   */
  public RowParser (String toolName, String serverUri, String sourceName) {
    this.toolName = toolName;
    this.serverUri = serverUri;
    this.sourceName = sourceName;
  }
  
  /**
   * Converts an array of String objects into an appropriate SensorData object. Since the order and
   * type of each String will differ for each kind of tabular data (for example each type of power
   * meter), a different implementation of this interface will be required for each type of meter.
   * 
   * @param col The row of the table, with each column represented as a String array element.
   * @return The new SensorData object.
   * @throws RowParseException If there are problems parsing the row.
   */
  public abstract SensorData parseRow(String[] col) throws RowParseException;
}
