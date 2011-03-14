package org.wattdepot.resource.energy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.Test;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Tests the Energy class.
 * 
 * @author Robert Brewer
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class TestEnergy {

  /**
   * Tests Energy constructor.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConstructor() {
    assertNull("Got valid Energy object with null straddle", new Energy(null, null, false));
  }

  /**
   * Tests the code for getting energy values.
   * 
   * @throws Exception If there are problems creating timestamps.
   */
  @Test
  public void testGetEnergy() throws Exception {
    XMLGregorianCalendar beforeTime, afterTime, timestamp;
    SensorData beforeData, afterData;
    String source = "http://server.wattdepot.org:1234/wattdepot/sources/foo-source";
    SensorDataStraddle straddle1, straddle2;
    Energy energy;

    // getEnergy for degenerate straddles with flat power generation
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 100.0, 0, 0, 0, false);
    timestamp = beforeTime;
    straddle1 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    beforeTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 100.0, 0, 100.0, 0, false);
    timestamp = beforeTime;
    straddle2 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    energy = new Energy(straddle1, straddle2, false);
    assertEquals("getEnergyGenerated on degenerate straddles with flat power was wrong", 100.0,
        energy.getEnergyGenerated(), 0.01);
    energy = new Energy(straddle1, straddle2, true);
    assertEquals("getEnergyGenerated on degenerate straddles with flat power was wrong", 100.0,
        energy.getEnergyGenerated(), 0.01);

    // Now power increases by double over one hour
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 100.0, 0, 0, 0, false);
    timestamp = beforeTime;
    straddle1 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    beforeTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 200.0, 0, 150, 0, false);
    timestamp = beforeTime;
    straddle2 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    energy = new Energy(straddle1, straddle2, false);
    assertEquals("getEnergyGenerated on degenerate straddles with doubling power was wrong", 150.0,
        energy.getEnergyGenerated(), 0.01);
    energy = new Energy(straddle1, straddle2, true);
    assertEquals("getEnergyGenerated on degenerate straddles with doubling power was wrong", 150.0,
        energy.getEnergyGenerated(), 0.01);

    // Now power decreases by half over one day
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 100.0, 0, 1234, 0, false);
    timestamp = beforeTime;
    straddle1 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    beforeTime = Tstamp.makeTimestamp("2009-07-29T08:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 50.0, 0, 3034, 0, false);
    timestamp = beforeTime;
    straddle2 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    energy = new Energy(straddle1, straddle2, false);
    assertEquals("getEnergyGenerated on degenerate straddles with doubling power was wrong",
        1800.0, energy.getEnergyGenerated(), 0.01);
    energy = new Energy(straddle1, straddle2, true);
    assertEquals("getEnergyGenerated on degenerate straddles with doubling power was wrong",
        1800.0, energy.getEnergyGenerated(), 0.01);
    assertTrue("Interpolated property not found", energy.getEnergy().isInterpolated());

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 5.5E7, 0, 5678, 0, false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source, 6.4E7, 0, 14880678, 0,
            false);
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    straddle1 = new SensorDataStraddle(timestamp, beforeData, afterData);
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:30:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:45:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 5.0E7, 0, 29130678, 0,
            false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source, 5.4E7, 0, 42130678, 0,
            false);
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:42:00.000-10:00");
    straddle2 = new SensorDataStraddle(timestamp, beforeData, afterData);
    energy = new Energy(straddle1, straddle2, false);
    assertEquals("getEnergyGenerated on Oscar data was wrong", 2.8033333333333332E7, energy
        .getEnergyGenerated(), 0.1);
    energy = new Energy(straddle1, straddle2, true);
    assertEquals("getEnergyGenerated on Oscar data was wrong", 2.6633333333333336E7, energy
        .getEnergyGenerated(), 0.1);
  }

  /**
   * Tests the behavior when an energy counter rolls over.
   * 
   * @throws Exception If there are problems creating timestamps.
   */
  @Test
  public void testCounterRollover() throws Exception {
    XMLGregorianCalendar beforeTime, afterTime, timestamp;
    SensorData beforeData, afterData;
    String source = "http://server.wattdepot.org:1234/wattdepot/sources/foo-source";
    SensorDataStraddle straddle1, straddle2;
    Energy energy;

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 5.5E7, 0, 5678, 0, false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source, 6.4E7, 0, 14880678, 0,
            false);
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    straddle1 = new SensorDataStraddle(timestamp, beforeData, afterData);
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:30:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:45:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 5.0E7, 0, 29130678, 0,
            false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source, 5.4E7, 0, 6789, 0, false);
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:42:00.000-10:00");
    straddle2 = new SensorDataStraddle(timestamp, beforeData, afterData);
    energy = new Energy(straddle1, straddle2, true);
    assertTrue("getEnergyGenerated on Oscar data returned positive value after counter rollover",
        energy.getEnergyGenerated() < 0);
  }
}
