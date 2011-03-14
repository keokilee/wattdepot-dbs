package org.wattdepot.resource.sensordata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.wattdepot.resource.sensordata.jaxb.SensorData.POWER_GENERATED;
import static org.wattdepot.resource.sensordata.jaxb.SensorData.ENERGY_GENERATED_TO_DATE;
import static org.wattdepot.resource.sensordata.jaxb.SensorData.ENERGY_CONSUMED_TO_DATE;
import java.util.ArrayList;
import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.Test;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Tests the SensorDataStraddle class.
 * 
 * @author Robert Brewer
 */
public class TestSensorDataStraddle {

  /**
   * Tests the constructor, ensuring that it does not accept invalid data.
   * 
   * @throws Exception If there are problems creating timestamps.
   */
  @Test
  @SuppressWarnings( { "PMD.EmptyCatchBlock", "PMD.AvoidDuplicateLiterals" })
  public void testConstructor() throws Exception {
    XMLGregorianCalendar time1 = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    XMLGregorianCalendar time2 = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    XMLGregorianCalendar time3 = Tstamp.makeTimestamp("2009-07-28T09:07:00.000-10:00");
    XMLGregorianCalendar time4 = Tstamp.makeTimestamp("2009-07-28T09:23:00.000-10:00");
    XMLGregorianCalendar time5 = Tstamp.makeTimestamp("2009-07-28T09:30:00.000-10:00");
    String tool = "JUnit";
    String source = "http://server.wattdepot.org:1234/wattdepot/sources/foo-source";
    SensorData data1 = new SensorData(time1, tool, source), data2 =
        new SensorData(time2, tool, source), data4 = new SensorData(time4, tool, source), data5 =
        new SensorData(time5, tool, source);

    // all null
    try {
      new SensorDataStraddle(null, null, null);
      fail("Able to create SensorDataStraddle with null beforeData");
    }
    catch (IllegalArgumentException e) {
      // expected in this case
    }
    // afterData null
    try {
      new SensorDataStraddle(null, data1, null);
      fail("Able to create SensorDataStraddle with null afterData");
    }
    catch (IllegalArgumentException e) {
      // expected in this case
    }
    // swapped order
    try {
      new SensorDataStraddle(null, data2, data1);
      fail("Able to create SensorDataStraddle with beforeData > afterData");
    }
    catch (IllegalArgumentException e) {
      // expected in this case
    }
    // timestamp before range
    try {
      new SensorDataStraddle(time1, data2, data5);
      fail("Able to create SensorDataStraddle with timestamp < beforeData");
    }
    catch (IllegalArgumentException e) {
      // expected in this case
    }
    // timestamp after range
    try {
      new SensorDataStraddle(time5, data1, data4);
      fail("Able to create SensorDataStraddle with timestamp > afterData");
    }
    catch (IllegalArgumentException e) {
      // expected in this case
    }
    // timestamp equal to before
    assertNotNull("Unable to create SensorDataStraddle with timestamp == beforeData",
        new SensorDataStraddle(time2, data2, data4));
    // timestamp equal to after
    assertNotNull("Unable to create SensorDataStraddle with timestamp == afterData",
        new SensorDataStraddle(time4, data2, data4));
    // degenerate case: timestamp == beforeData == afterData
    assertNotNull("Unable to create SensorDataStraddle with timestamp == beforeData == afterData",
        new SensorDataStraddle(time2, data2, data2));
    // timestamp right in middle
    assertNotNull("Unable to create SensorDataStraddle with timestamp in range",
        new SensorDataStraddle(time3, data1, data5));
  }

  /**
   * Tests the interpolation code for getting power values.
   * 
   * @throws Exception If there are problems creating timestamps.
   */
  @Test
  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  public void testGetPower() throws Exception {
    XMLGregorianCalendar beforeTime, afterTime, timestamp;
    SensorData beforeData, afterData;
    String tool = "JUnit";
    String source = "http://server.wattdepot.org:1234/wattdepot/sources/foo-source";
    SensorDataStraddle straddle;
    double interpolatedPower;

    // timestamp == beforeData == afterData, getPower should just return beforeData
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData = new SensorData(beforeTime, tool, source, new Property(POWER_GENERATED, "100"));
    timestamp = beforeTime;
    straddle = new SensorDataStraddle(timestamp, beforeData, beforeData);
    assertEquals("getPower on degenerate straddle did not return beforeData", straddle.getPower(),
        beforeData);

    // slope is 2 (100 W difference in 50 seconds)
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-07-28T08:00:50.000-10:00");
    beforeData = new SensorData(beforeTime, tool, source, new Property(POWER_GENERATED, "100"));
    afterData = new SensorData(afterTime, tool, source, new Property(POWER_GENERATED, "200"));
    timestamp = Tstamp.makeTimestamp("2009-07-28T08:00:25.000-10:00");
    straddle = new SensorDataStraddle(timestamp, beforeData, afterData);
    interpolatedPower =
        Double.valueOf(straddle.getPower().getProperties().getProperty().get(0).getValue());
    assertEquals("Interpolated power did not equal expected value", 150, interpolatedPower, 0.01);
    assertTrue("Interpolated property not found", straddle.getPower().isInterpolated());

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData = new SensorData(beforeTime, tool, source, new Property(POWER_GENERATED, "5.5E7"));
    afterData = new SensorData(afterTime, tool, source, new Property(POWER_GENERATED, "6.4E7"));
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    straddle = new SensorDataStraddle(timestamp, beforeData, afterData);
    interpolatedPower =
        Double.valueOf(straddle.getPower().getProperties().getProperty().get(0).getValue());
    // System.out.println(interpolatedPower);
    assertEquals("Interpolated power did not equal expected value", 6.28E7, interpolatedPower, 0.01);
    assertTrue("Interpolated property not found", straddle.getPower().isInterpolated());
  }

  /**
   * Tests the interpolation code for getting energy counter values.
   * 
   * @throws Exception If there are problems creating timestamps.
   */
  @Test
  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  public void testGetEnergy() throws Exception {
    XMLGregorianCalendar beforeTime, afterTime, timestamp;
    SensorData beforeData, afterData;
    String tool = "JUnit";
    String source = "http://server.wattdepot.org:1234/wattdepot/sources/foo-source";
    SensorDataStraddle straddle;
    double interpolatedEnergy;

    // timestamp == beforeData == afterData, getEnergy should just return beforeData
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData =
        new SensorData(beforeTime, tool, source, new Property(ENERGY_GENERATED_TO_DATE, "100"));
    timestamp = beforeTime;
    straddle = new SensorDataStraddle(timestamp, beforeData, beforeData);
    assertEquals("getEnergyGeneratedToDate on degenerate straddle did not return beforeData",
        straddle.getEnergyGeneratedToDate(), 100, 0.001);

    // slope is 2 (100 Wh difference in 50 seconds)
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-07-28T08:00:50.000-10:00");
    beforeData =
        new SensorData(beforeTime, tool, source, new Property(ENERGY_GENERATED_TO_DATE, "100"));
    afterData =
        new SensorData(afterTime, tool, source, new Property(ENERGY_GENERATED_TO_DATE, "200"));
    timestamp = Tstamp.makeTimestamp("2009-07-28T08:00:25.000-10:00");
    straddle = new SensorDataStraddle(timestamp, beforeData, afterData);
    interpolatedEnergy = Double.valueOf(straddle.getEnergyGeneratedToDate());
    assertEquals("Interpolated energy did not equal expected value", 150, interpolatedEnergy, 0.01);

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData =
        new SensorData(beforeTime, tool, source, new Property(ENERGY_GENERATED_TO_DATE, "55000000"));
    afterData =
        new SensorData(afterTime, tool, source, new Property(ENERGY_GENERATED_TO_DATE, "64000000"));
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    straddle = new SensorDataStraddle(timestamp, beforeData, afterData);
    interpolatedEnergy = Double.valueOf(straddle.getEnergyGeneratedToDate());
    assertEquals("Interpolated energy did not equal expected value", 62800000, interpolatedEnergy,
        0.01);
    assertEquals("Missing property did not result in zero energy value", 0, straddle
        .getEnergyConsumedToDate(), 0.001);

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData =
        new SensorData(beforeTime, tool, source, new Property(ENERGY_CONSUMED_TO_DATE, "55000000"));
    afterData =
        new SensorData(afterTime, tool, source, new Property(ENERGY_CONSUMED_TO_DATE, "64000000"));
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    straddle = new SensorDataStraddle(timestamp, beforeData, afterData);
    interpolatedEnergy = Double.valueOf(straddle.getEnergyConsumedToDate());
    assertEquals("Interpolated energy did not equal expected value", 62800000, interpolatedEnergy,
        0.01);
  }

  /**
   * Tests the static method that consolidates results from a list of straddles.
   * 
   * @throws Exception If there are problems creating timestamps.
   */
  @Test
  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  public void testGetPowerFromList() throws Exception {
    XMLGregorianCalendar beforeTime, afterTime, timestamp;
    SensorData beforeData, afterData, powerData;
    String tool = "JUnit";
    String source = "http://server.wattdepot.org:1234/wattdepot/sources/foo-source";
    SensorDataStraddle straddle1, straddle2;
    double interpolatedPower = -1;

    // timestamp == beforeData == afterData for two straddles, getPower should return beforeData * 2
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData = new SensorData(beforeTime, tool, source, new Property(POWER_GENERATED, "100"));
    timestamp = beforeTime;
    straddle1 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    List<SensorDataStraddle> straddleList = new ArrayList<SensorDataStraddle>();
    straddleList.add(straddle1);
    straddleList.add(straddle1);
    powerData = SensorDataStraddle.getPowerFromList(straddleList, source);
    // System.out.println(powerData);
    interpolatedPower = powerData.getProperties().getPropertyAsDouble(POWER_GENERATED);
    assertEquals("getPower for virtual source on degenerate straddle did not return beforeData",
        200, interpolatedPower, 0.01);
    assertFalse("Interpolated property found on non-interpolated data", straddle1.getPower()
        .isInterpolated());

    // Simple, in the middle of interval
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:12:35.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:13:25.000-10:00");
    beforeData = new SensorData(beforeTime, tool, source, new Property(POWER_GENERATED, "1.0E7"));
    afterData = new SensorData(afterTime, tool, source, new Property(POWER_GENERATED, "2.0E7"));
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    straddle1 = new SensorDataStraddle(timestamp, beforeData, afterData);
    interpolatedPower =
        Double.valueOf(straddle1.getPower().getProperties().getProperty().get(0).getValue());
    assertEquals("Interpolated power did not equal expected value", 1.5E7, interpolatedPower, 0.01);
    assertTrue("Interpolated property not found", straddle1.getPower().isInterpolated());

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData = new SensorData(beforeTime, tool, source, new Property(POWER_GENERATED, "5.5E7"));
    afterData = new SensorData(afterTime, tool, source, new Property(POWER_GENERATED, "6.4E7"));
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    straddle2 = new SensorDataStraddle(timestamp, beforeData, afterData);
    interpolatedPower =
        Double.valueOf(straddle2.getPower().getProperties().getProperty().get(0).getValue());
    // System.out.println(interpolatedPower);
    assertEquals("Interpolated power did not equal expected value", 6.28E7, interpolatedPower, 0.01);
    assertTrue("Interpolated property not found", straddle2.getPower().isInterpolated());

    // Now make list of straddle1 & straddle2 and confirm results are combined
    straddleList.clear();
    straddleList.add(straddle1);
    straddleList.add(straddle2);
    powerData = SensorDataStraddle.getPowerFromList(straddleList, source);
    // System.out.println(powerData);
    interpolatedPower = powerData.getProperties().getPropertyAsDouble(POWER_GENERATED);
    assertEquals("Interpolated power did not equal expected value", 7.78E7, interpolatedPower, 0.01);
  }
}
