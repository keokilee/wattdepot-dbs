package org.wattdepot.datainput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Tests the VerisRowParser class.
 * 
 * @author Robert Brewer
 */
public class TestVerisRowParser {

  private static final String SOURCE_NAME = "foo-source";
  private static final String TOOL_NAME = "JUnit";
  private static final String SERVER_URI = "http://server.wattdepot.org:1234/wattdepot/";

  /**
   * Tests whether a valid row is parsed properly into a SensorData object.
   * 
   * @throws Exception If there are problems converting the timestamp.
   */
  @Test
  public void testValidRow() throws Exception {
    RowParser parser = new VerisRowParser(TOOL_NAME, SERVER_URI, SOURCE_NAME);
    // Example row from 2mauka meter
    String row = "2009-09-01 01:01:01\t0\t0\t0\t37785.62\t3.719";
    SensorData data =
      new SensorData(Tstamp.makeTimestamp("2009-09-01T01:01:01.000-10:00"), TOOL_NAME, Source
          .sourceToUri(SOURCE_NAME, SERVER_URI));
    Property powerConsumed = new Property(SensorData.POWER_CONSUMED, "3719.0");
    Property energyConsumed = new Property(SensorData.ENERGY_CONSUMED_TO_DATE, "3.778562E7");
    data.addProperty(powerConsumed);
    data.addProperty(energyConsumed);
    SensorData parsedData = parser.parseRow(row.split("\t"));
    assertEquals("Parsed sensor data differs from hand-created sensor data", data, parsedData);
  }

  /**
   * Tests whether a valid row is parsed properly into a SensorData object.
   * 
   * @throws RowParseException If there are problems parsing the row.
   */
  @Test(expected = RowParseException.class)
  public void testMissingColumns() throws RowParseException {
    RowParser parser = new VerisRowParser(TOOL_NAME, SERVER_URI, SOURCE_NAME);
    // Example row from 5mauka meter
    String row = "2009-05-02 08:00:02\t160";
    SensorData parsedData = parser.parseRow(row.split("\t"));
    assertNull("Row with missing columns was successfully parsed", parsedData);
  }

  /**
   * Tests whether a valid row is parsed properly into a SensorData object.
   * 
   * @throws RowParseException If there are problems parsing the row.
   */
  @Test(expected = RowParseException.class)
  public void testEmptyRow() throws RowParseException {
    RowParser parser = new VerisRowParser(TOOL_NAME, SERVER_URI, SOURCE_NAME);
    String[] row = { "" };
    SensorData parsedData = parser.parseRow(row);
    assertNull("Empty string row was parsed successfully", parsedData);
  }

  /**
   * Tests whether a valid row is parsed properly into a SensorData object.
   * 
   * @throws RowParseException If there are problems parsing the row.
   */
  @Test(expected = RowParseException.class)
  public void testNullRow() throws RowParseException {
    RowParser parser = new VerisRowParser(TOOL_NAME, SERVER_URI, SOURCE_NAME);
    String[] row = null;
    SensorData parsedData = parser.parseRow(row);
    assertNull("Null row was parsed successfully", parsedData);
  }

  /**
   * Tests whether a valid row is parsed properly into a SensorData object.
   */
  @Test
  public void testBogusColumns() {
    RowParser parser = new VerisRowParser(TOOL_NAME, SERVER_URI, SOURCE_NAME);
    String row;
    row = "time (US/Hawaii)\t0\t0\t0\t43770\t3.5";
    try {
      parser.parseRow(row.split("\t"));
      fail("Row with bogus time column was successfully parsed");
    }
    catch (RowParseException e) { // NOPMD
      // Do nothing, this is expected behavior
    }
    row = "2009-05-01 00:00:03\t0\t0\t0\tEnergy Consumption (kWh)\t3.5";
    try {
      parser.parseRow(row.split("\t"));
      fail("Row with bogus energyConsumed column was successfully parsed");
    }
    catch (RowParseException e) { // NOPMD
      // Do nothing, this is expected behavior
    }
    row = "2009-05-01 00:00:03\t0\t0\t0\t43770\tReal Power (kW)";
    try {
      parser.parseRow(row.split("\t"));
      fail("Row with bogus powerConsumed column was successfully parsed");
    }
    catch (RowParseException e) { // NOPMD
      // Do nothing, this is expected behavior
    }
  }
}
