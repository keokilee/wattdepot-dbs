package org.wattdepot.resource.carbon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.Test;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Tests the Carbon class.
 * 
 * @author Robert Brewer
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class TestCarbon {

  /**
   * Tests the code for getting carbon values.
   * 
   * @throws Exception If there are problems creating timestamps.
   */
  @Test
  public void testGetCarbon() throws Exception {
    XMLGregorianCalendar beforeTime, afterTime, timestamp;
    SensorData beforeData, afterData;
    String source = "http://server.wattdepot.org:1234/wattdepot/sources/foo-source";
    double carbonIntensity = 1000.0;
    SensorDataStraddle straddle1, straddle2;
    Carbon carbon;

    // getEnergy for degenerate straddles with flat power generation
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 100.0, 0, false);
    timestamp = beforeTime;
    straddle1 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    beforeTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 100.0, 0, false);
    timestamp = beforeTime;
    straddle2 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    carbon = new Carbon(straddle1, straddle2, carbonIntensity, false);
    assertEquals("getCarbonEmitted on degenerate straddles with flat power was wrong", 0.1, carbon
        .getCarbonEmitted(), 0.001);

    // Now power increases by double over one hour
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 100.0, 0, false);
    timestamp = beforeTime;
    straddle1 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    beforeTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 200.0, 0, false);
    timestamp = beforeTime;
    straddle2 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    carbon = new Carbon(straddle1, straddle2, carbonIntensity, false);
    assertEquals("getCarbonEmitted on degenerate straddles with doubling power was wrong", 0.15,
        carbon.getCarbonEmitted(), 0.001);

    // Now power decreases by half over one day
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 100.0, 0, false);
    timestamp = beforeTime;
    straddle1 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    beforeTime = Tstamp.makeTimestamp("2009-07-29T08:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 50.0, 0, false);
    timestamp = beforeTime;
    straddle2 = new SensorDataStraddle(timestamp, beforeData, beforeData);
    carbon = new Carbon(straddle1, straddle2, carbonIntensity, false);
    assertEquals("getCarbonEmitted on degenerate straddles with doubling power was wrong", 1.8,
        carbon.getCarbonEmitted(), 0.01);
    assertTrue("Interpolated property not found", carbon.getEnergy().isInterpolated());

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 5.5E7, 0, false);
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source, 6.4E7, 0, false);
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    straddle1 = new SensorDataStraddle(timestamp, beforeData, afterData);
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:30:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:45:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 5.0E7, 0, false);
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source, 5.4E7, 0, false);
    timestamp = Tstamp.makeTimestamp("2009-10-12T00:42:00.000-10:00");
    straddle2 = new SensorDataStraddle(timestamp, beforeData, afterData);
    carbon = new Carbon(straddle1, straddle2, carbonIntensity, false);
    assertEquals("getCarbonEmitted on Oscar data was wrong", 28033.3333333333332, carbon
        .getCarbonEmitted(), 0.01);
  }
}
