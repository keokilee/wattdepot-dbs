package org.wattdepot.resource.carbon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.Test;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.test.ServerTestHelper;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Tests the Carbon resource API at the HTTP level using WattDepotClient.
 * 
 * @author Robert Brewer
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class TestCarbonResource extends ServerTestHelper {

  // TODO Skipping authentication tests for now
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: public Source with no credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test
  // public void testPowerPublicWithNoCredentials() throws WattDepotClientException {
  // WattDepotClient client = new WattDepotClient(getHostName());
  // assertNotNull(MISSING_SENSORDATAREFS, client.getSensorDataIndex(defaultPublicSource)
  // .getSensorDataRef());
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: public Source with invalid
  // credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test(expected = NotAuthorizedException.class)
  // public void testFullIndexPublicBadAuth() throws WattDepotClientException {
  // // Shouldn't authenticate with invalid credentials.
  // WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, "foo");
  // client.getSensorDataIndex(defaultPublicSource);
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: public Source with valid admin
  // * credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test
  // public void testFullIndexPublicWithAdminCredentials() throws WattDepotClientException {
  // WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, adminPassword);
  // assertNotNull(MISSING_SENSORDATAREFS, client.getSensorDataIndex(defaultPublicSource)
  // .getSensorDataRef());
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: public Source with valid owner
  // * credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test
  // public void testFullIndexPublicWithOwnerCredentials() throws WattDepotClientException {
  // WattDepotClient client =
  // new WattDepotClient(getHostName(), defaultOwnerUsername,
  // defaultOwnerPassword);
  // assertNotNull(MISSING_SENSORDATAREFS, client.getSensorDataIndex(defaultPublicSource)
  // .getSensorDataRef());
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: public Source with valid non-owner
  // * credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test
  // public void testFullIndexPublicWithNonOwnerCredentials() throws WattDepotClientException {
  // WattDepotClient client =
  // new WattDepotClient(getHostName(), defaultNonOwnerUsername,
  // defaultNonOwnerPassword);
  // assertNotNull(MISSING_SENSORDATAREFS, client.getSensorDataIndex(defaultPublicSource)
  // .getSensorDataRef());
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: private Source with no credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test(expected = NotAuthorizedException.class)
  // public void testFullIndexPrivateWithNoCredentials() throws WattDepotClientException {
  // WattDepotClient client = new WattDepotClient(getHostName());
  // client.getSensorDataIndex(defaultPrivateSource);
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: private Source with invalid
  // credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test(expected = NotAuthorizedException.class)
  // public void testFullIndexPrivateBadAuth() throws WattDepotClientException {
  // // Shouldn't authenticate with no username or password
  // WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, "wrong-password");
  // client.getSensorDataIndex(defaultPrivateSource);
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: private Source with admin credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test
  // public void testFullIndexPrivateAdminAuth() throws WattDepotClientException {
  // // Shouldn't authenticate with no username or password
  // WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, adminPassword);
  // assertNotNull(MISSING_SENSORDATAREFS,
  // client.getSensorDataIndex(defaultPrivateSource));
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: private Source with owner credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test
  // public void testFullIndexPrivateOwnerAuth() throws WattDepotClientException {
  // // Shouldn't authenticate with no username or password
  // WattDepotClient client =
  // new WattDepotClient(getHostName(), defaultOwnerUsername,
  // defaultOwnerPassword);
  // assertNotNull(MISSING_SENSORDATAREFS,
  // client.getSensorDataIndex(defaultPrivateSource));
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: private Source with non-owner
  // * credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test(expected = NotAuthorizedException.class)
  // public void testFullIndexPrivateNonOwnerAuth() throws WattDepotClientException {
  // // Shouldn't authenticate with no username or password
  // WattDepotClient client =
  // new WattDepotClient(getHostName(), defaultNonOwnerUsername,
  // defaultNonOwnerPassword);
  // client.getSensorDataIndex(defaultPrivateSource);
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: unknown Source name with no
  // credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test(expected = ResourceNotFoundException.class)
  // public void testFullIndexBadSourceNameAnon() throws WattDepotClientException {
  // // Shouldn't authenticate with no username or password
  // WattDepotClient client = new WattDepotClient(getHostName());
  // client.getSensorDataIndex("bogus-source-name");
  // }
  //
  // /**
  // * Tests retrieval of all SensorData from a Source. Type: unknown Source name with valid
  // * credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test(expected = ResourceNotFoundException.class)
  // public void testFullIndexBadSourceNameAuth() throws WattDepotClientException {
  // // Shouldn't authenticate with no username or password
  // WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, adminPassword);
  // client.getSensorDataIndex("bogus-source-name");
  // }
  //
  // /**
  // * Tests that a Source starts with no SensorData. Type: public Source with valid owner
  // * credentials.
  // *
  // * @throws WattDepotClientException If problems are encountered
  // */
  // @Test
  // public void testFullIndexStartsEmpty() throws WattDepotClientException {
  // WattDepotClient client =
  // new WattDepotClient(getHostName(), defaultOwnerUsername,
  // defaultOwnerPassword);
  // assertTrue("Fresh DB contains SensorData", client.getSensorDataIndex(
  // defaultPublicSource).getSensorDataRef().isEmpty());
  // }
  //
  // /**
  // * Tests that after storing SensorData to a Source, the SensorDataIndex corresponds to the data
  // * that has been stored. Type: public Source with valid owner credentials.
  // *
  // * @throws Exception If stuff goes wrong.
  // */
  // @Test
  // public void testFullIndexAfterStores() throws Exception {
  // WattDepotClient client =
  // new WattDepotClient(getHostName(), defaultOwnerUsername,
  // defaultOwnerPassword);
  // SensorData data1 = makeTestSensorData1(), data2 = makeTestSensorData2(), data3 =
  // makeTestSensorData3();
  // assertTrue(DATA_STORE_FAILED, client.storeSensorData(data1));
  // List<SensorDataRef> index =
  // client.getSensorDataIndex(defaultPublicSource).getSensorDataRef();
  // assertEquals("Wrong number of SensorDataRefs after store", 1, index.size());
  // assertTrue("getSensorDataIndex didn't return expected SensorDataRef",
  // sensorDataRefEqualsSensorData(index.get(0), data1));
  // assertTrue(DATA_STORE_FAILED, client.storeSensorData(data2));
  // index = client.getSensorDataIndex(defaultPublicSource).getSensorDataRef();
  // assertEquals("Wrong number of SensorDataRefs after store", 2, index.size());
  // List<SensorData> origData = new ArrayList<SensorData>();
  // origData.add(data1);
  // origData.add(data2);
  // assertTrue("getSensorDataIndex didn't return expected SensorDataRefs",
  // compareSensorDataRefsToSensorDatas(index, origData));
  // assertTrue(DATA_STORE_FAILED, client.storeSensorData(data3));
  // index = client.getSensorDataIndex(defaultPublicSource).getSensorDataRef();
  // assertEquals("Wrong number of SensorDataRefs after store", 3, index.size());
  // origData.add(data3);
  // assertTrue("getSensorDataIndex didn't return expected SensorDataRefs",
  // compareSensorDataRefsToSensorDatas(index, origData));
  // }

  // Tests for GET {host}/sources/{source}/carbon/{timestamp}

  /**
   * Tests the carbon resource on a non-virtual source.
   * 
   * @throws Exception If there are problems creating timestamps, or if the client has problems.
   */
  @Test
  public void testGetCarbon() throws Exception {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultOwnerUsername,
            defaultOwnerPassword);

    XMLGregorianCalendar beforeTime, afterTime, timestamp1, timestamp2;
    SensorData beforeData, afterData;
    String source = Source.sourceToUri(defaultPublicSource, server);
    String sourceName = defaultPublicSource;

    // timestamp = range for flat power, getCarbon should just return simple carbon value
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 100, 0, false);
    client.storeSensorData(beforeData);
    afterTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source, 100, 0, false);
    client.storeSensorData(afterData);
    assertEquals("getCarbonEmitted on degenerate range with default interval gave wrong value",
        0.1, client.getCarbonEmitted(sourceName, beforeTime, afterTime, 0), 0.01);
    assertEquals("getCarbonEmitted on degenerate range with 2 minute interval gave wrong value",
        0.1, client.getCarbonEmitted(sourceName, beforeTime, afterTime, 2), 0.01);
    assertEquals("getCarbonEmitted on degenerate range with 5 minute interval gave wrong value",
        0.1, client.getCarbonEmitted(sourceName, beforeTime, afterTime, 5), 0.01);
    assertEquals("getCarbonEmitted on degenerate range with 5 minute interval gave wrong value",
        0.1, client.getCarbonEmitted(sourceName, beforeTime, afterTime, 30), 0.01);
    assertEquals("getCarbonEmitted on degenerate range with 5 minute interval gave wrong value",
        0.1, client.getCarbonEmitted(sourceName, beforeTime, afterTime, 29), 0.01);
    // Try with interval too big
    try {
      client.getCarbonEmitted(sourceName, beforeTime, afterTime, 61);
      fail("getCarbon worked with interval longer than range");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }
    // Try with range that extends beyond sensor data
    XMLGregorianCalendar tooEarly = Tstamp.makeTimestamp("2008-07-28T08:00:00.000-10:00");
    XMLGregorianCalendar tooLate = Tstamp.makeTimestamp("2010-07-28T08:00:00.000-10:00");
    try {
      client.getCarbonEmitted(sourceName, tooEarly, tooLate, 0);
      fail("getCarbon worked with range outside sensor data");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }
    client.deleteSensorData(sourceName, beforeData.getTimestamp());
    client.deleteSensorData(sourceName, afterData.getTimestamp());

    // slope is 2 (100 W difference in 1 hour)
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 100, 0, false);
    afterTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source, 200, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    assertEquals("getCarbon on degenerate range with default interval gave wrong value", 0.15,
        client.getCarbonEmitted(sourceName, beforeTime, afterTime, 0), 0.001);
    assertEquals("getCarbon on degenerate range with default interval gave wrong value", 0.15,
        client.getCarbonEmitted(sourceName, beforeTime, afterTime, 5), 0.001);
    assertTrue("Interpolated property not found", client.getCarbon(sourceName, beforeTime,
        afterTime, 0).isInterpolated());
    client.deleteSensorData(sourceName, beforeData.getTimestamp());
    client.deleteSensorData(sourceName, afterData.getTimestamp());

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 5.5E7, 0, false);
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source, 6.4E7, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    timestamp1 = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:30:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:45:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source, 5.0E7, 0, false);
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source, 5.4E7, 0, false);
    timestamp2 = Tstamp.makeTimestamp("2009-10-12T00:42:00.000-10:00");
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    // TODO Seems like these should be closer, but haven't done the hand calculations to confirm
    assertEquals("getCarbonEmitted on on Oscar data was wrong", 2.8033333333333332E4, client
        .getCarbonEmitted(sourceName, timestamp1, timestamp2, 0), 0.3E4);
    SensorData energyData = client.getCarbon(sourceName, timestamp1, timestamp2, 1);
    assertEquals("getCarbon on on Oscar data was wrong", 2.8033333333333332E4, energyData
        .getProperties().getPropertyAsDouble(SensorData.CARBON_EMITTED), 0.2E4);
  }

  /**
   * Tests the energy resource on a virtual source.
   * 
   * @throws Exception If there are problems creating timestamps, or if the client has problems.
   */
  @Test
  public void testGetVirtualSourceCarbon() throws Exception {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultOwnerUsername,
            defaultOwnerPassword);

    XMLGregorianCalendar beforeTime, afterTime, timestamp1, timestamp2;
    SensorData beforeData, afterData;
    String source1Name = defaultPublicSource;
    String source2Name = defaultPrivateSource;
    String virtualSourceName = defaultVirtualSource;
    String source1 = Source.sourceToUri(source1Name, server);
    String source2 = Source.sourceToUri(source2Name, server);

    // timestamp = range for flat power on both sources
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source1, 100, 0, false);
    client.storeSensorData(beforeData);
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source2, 100, 0, false);
    client.storeSensorData(beforeData);
    afterTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source1, 100, 0, false);
    client.storeSensorData(afterData);
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source2, 100, 0, false);
    client.storeSensorData(afterData);
    assertEquals("getCarbon on degenerate range with default interval gave wrong value", 0.4,
        client.getCarbonEmitted(virtualSourceName, beforeTime, afterTime, 0), 0.01);
    assertEquals("getCarbon on degenerate range with 2 minute interval gave wrong value", 0.4,
        client.getCarbonEmitted(virtualSourceName, beforeTime, afterTime, 2), 0.01);
    assertEquals("getCarbon on degenerate range with 5 minute interval gave wrong value", 0.4,
        client.getCarbonEmitted(virtualSourceName, beforeTime, afterTime, 5), 0.01);
    assertEquals("getCarbon on degenerate range with 5 minute interval gave wrong value", 0.4,
        client.getCarbonEmitted(virtualSourceName, beforeTime, afterTime, 30), 0.01);
    assertEquals("getCarbon on degenerate range with 5 minute interval gave wrong value", 0.4,
        client.getCarbonEmitted(virtualSourceName, beforeTime, afterTime, 29), 0.01);
    try {
      client.getCarbonEmitted(virtualSourceName, beforeTime, afterTime, 61);
      fail("getCarbon worked with interval longer than range");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }
    // Try with range that extends beyond sensor data
    XMLGregorianCalendar tooEarly = Tstamp.makeTimestamp("2008-07-28T08:00:00.000-10:00");
    XMLGregorianCalendar tooLate = Tstamp.makeTimestamp("2010-07-28T08:00:00.000-10:00");
    try {
      client.getCarbonEmitted(virtualSourceName, tooEarly, tooLate, 0);
      fail("getCarbon worked with range outside sensor data");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }
    assertTrue("Interpolated property not found", client.getCarbon(virtualSourceName, beforeTime,
        afterTime, 0).isInterpolated());
    client.deleteSensorData(source1Name, beforeData.getTimestamp());
    client.deleteSensorData(source1Name, afterData.getTimestamp());
    client.deleteSensorData(source2Name, beforeData.getTimestamp());
    client.deleteSensorData(source2Name, afterData.getTimestamp());

    // Simple, in the middle of interval
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:12:35.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source1, 1.0E7, 0, false);
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:13:25.000-10:00");
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source1, 2.0E7, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    timestamp1 = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");

    beforeTime = Tstamp.makeTimestamp("2009-10-12T01:12:35.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source1, 1.0E7, 0, false);
    afterTime = Tstamp.makeTimestamp("2009-10-12T01:13:25.000-10:00");
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source1, 2.0E7, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    timestamp2 = Tstamp.makeTimestamp("2009-10-12T01:13:00.000-10:00");
    assertEquals("getCarbonEmitted on for simple gave wrong value", 1.5E4, client.getCarbonEmitted(
        source1Name, timestamp1, timestamp2, 0), 0.01);

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source2, 5.5E7, 0, false);
    client.storeSensorData(beforeData);
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source2, 6.4E7, 0, false);
    client.storeSensorData(afterData);

    beforeTime = Tstamp.makeTimestamp("2009-10-12T01:00:00.000-10:00");
    beforeData = SensorDataStraddle.makePowerSensorData(beforeTime, source2, 5.5E7, 0, false);
    client.storeSensorData(beforeData);
    afterTime = Tstamp.makeTimestamp("2009-10-12T01:15:00.000-10:00");
    afterData = SensorDataStraddle.makePowerSensorData(afterTime, source2, 6.4E7, 0, false);
    client.storeSensorData(afterData);
    assertEquals("getCarbonEmitted on for simple gave wrong value", 178440, client
        .getCarbonEmitted(source2Name, timestamp1, timestamp2, 0), 0.1);

    // Virtual source should get the sum of the two previous power values
    assertEquals("energy for virtual source did not equal expected value", 193440, client
        .getCarbonEmitted(virtualSourceName, timestamp1, timestamp2, 0), 0.01);
  }
}
