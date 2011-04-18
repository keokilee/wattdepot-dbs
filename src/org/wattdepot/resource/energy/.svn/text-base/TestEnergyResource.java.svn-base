package org.wattdepot.resource.energy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.Test;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.MiscClientException;
import org.wattdepot.client.NotAuthorizedException;
import org.wattdepot.client.OverwriteAttemptedException;
import org.wattdepot.client.ResourceNotFoundException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.test.ServerTestHelper;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Tests the Energy resource API at the HTTP level using WattDepotClient.
 * 
 * @author Robert Brewer
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class TestEnergyResource extends ServerTestHelper {

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

  // Tests for GET {host}/sources/{source}/energy/{timestamp}

  /**
   * Tests the energy resource on a non-virtual source.
   * 
   * @throws Exception If there are problems creating timestamps, or if the client has problems.
   */
  @Test
  public void testGetEnergy() throws Exception {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultOwnerUsername, defaultOwnerPassword);

    XMLGregorianCalendar beforeTime, afterTime, timestamp1, timestamp2;
    SensorData beforeData, afterData;
    String source = Source.sourceToUri(defaultPublicSource, server);
    String sourceName = defaultPublicSource;

    // timestamp = range for flat power, getEnergy should just return simple energy value
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 100, 0, 2345, 0, false);
    client.storeSensorData(beforeData);
    afterTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source, 100, 0, 2445, 0, false);
    client.storeSensorData(afterData);
    try {
      client.getEnergyGenerated(sourceName, beforeTime, afterTime, 61);
      fail("getEnergy worked with interval longer than range");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }
    assertEquals("getEnergy on degenerate range with default interval gave wrong value", 100,
        client.getEnergyGenerated(sourceName, beforeTime, afterTime, 0), 0.01);
    assertEquals("getEnergy on degenerate range with 2 minute interval gave wrong value", 100,
        client.getEnergyGenerated(sourceName, beforeTime, afterTime, 2), 0.01);
    assertEquals("getEnergy on degenerate range with 5 minute interval gave wrong value", 100,
        client.getEnergyGenerated(sourceName, beforeTime, afterTime, 5), 0.01);
    assertEquals("getEnergy on degenerate range with 30 minute interval gave wrong value", 100,
        client.getEnergyGenerated(sourceName, beforeTime, afterTime, 30), 0.01);
    assertEquals("getEnergy on degenerate range with 29 minute interval gave wrong value", 100,
        client.getEnergyGenerated(sourceName, beforeTime, afterTime, 29), 0.01);
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, sourceName));
    assertEquals("getEnergy with counters gave wrong value", 100, client.getEnergyGenerated(
        sourceName, beforeTime, afterTime, 0), 0.01);
    // interval value should be ignored completely
    assertEquals("getEnergy with counters gave wrong value", 100, client.getEnergyGenerated(
        sourceName, beforeTime, afterTime, 5), 0.01);
    // Back non-energy counter source properties
    assertTrue("Unable to remove energy counters", removeEnergyCounterProperty(client, sourceName));

    // Try with range that extends beyond sensor data
    XMLGregorianCalendar tooEarly = Tstamp.makeTimestamp("2008-07-28T08:00:00.000-10:00");
    XMLGregorianCalendar tooLate = Tstamp.makeTimestamp("2010-07-28T08:00:00.000-10:00");
    try {
      client.getEnergyGenerated(sourceName, tooEarly, tooLate, 0);
      fail("getEnergyGenerated worked with range outside sensor data");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, sourceName));
    try {
      client.getEnergyGenerated(sourceName, tooEarly, tooLate, 0);
      fail("getEnergyGenerated worked with range outside sensor data");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }
    assertTrue("Unable to remove energy counters", removeEnergyCounterProperty(client, sourceName));
    client.deleteSensorData(sourceName, beforeData.getTimestamp());
    client.deleteSensorData(sourceName, afterData.getTimestamp());

    // slope is 2 (100 W difference in 1 hour)
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 100, 0, 777777, 0, false);
    afterTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source, 200, 0, 777927, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    assertEquals("getEnergy on degenerate range with default interval gave wrong value", 150,
        client.getEnergyGenerated(sourceName, beforeTime, afterTime, 0), 0.01);
    assertEquals("getEnergy on degenerate range with default interval gave wrong value", 150,
        client.getEnergyGenerated(sourceName, beforeTime, afterTime, 5), 0.01);
    assertTrue("Interpolated property not found", client.getEnergy(sourceName, beforeTime,
        afterTime, 0).isInterpolated());
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, sourceName));
    assertEquals("getEnergy on degenerate range with default interval gave wrong value", 150,
        client.getEnergyGenerated(sourceName, beforeTime, afterTime, 0), 0.01);
    assertTrue("Unable to remove energy counters", removeEnergyCounterProperty(client, sourceName));
    client.deleteSensorData(sourceName, beforeData.getTimestamp());
    client.deleteSensorData(sourceName, afterData.getTimestamp());

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 5.5E7, 0, 5678, 0, false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source, 6.4E7, 0, 14880678, 0,
            false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    timestamp1 = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:30:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:45:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source, 5.0E7, 0, 29130678, 0,
            false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source, 5.4E7, 0, 42130678, 0,
            false);
    timestamp2 = Tstamp.makeTimestamp("2009-10-12T00:42:00.000-10:00");
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    assertEquals("getEnergyGenerated on on Oscar data was wrong", 2.8033333333333332E7, client
        .getEnergyGenerated(sourceName, timestamp1, timestamp2, 0), 0.2E7);
    assertEquals("getEnergyConsumed on on Oscar data was wrong", 0, client.getEnergyConsumed(
        sourceName, timestamp1, timestamp2, 0), 0.01);
    SensorData energyData = client.getEnergy(sourceName, timestamp1, timestamp2, 1);
    assertEquals("getEnergy on on Oscar data was wrong", 2.8033333333333332E7, energyData
        .getProperties().getPropertyAsDouble(SensorData.ENERGY_GENERATED), 0.2E7);
    assertEquals("getEnergy on on Oscar data was wrong", 0, energyData.getProperties()
        .getPropertyAsDouble(SensorData.ENERGY_CONSUMED), 0.01);

    // Now using counters
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, sourceName));
    assertEquals("getEnergyGenerated on on Oscar data was wrong", 2.6633333333333336E7, client
        .getEnergyGenerated(sourceName, timestamp1, timestamp2, 0), 0.01);
    assertEquals("getEnergyConsumed on on Oscar data was wrong", 0, client.getEnergyConsumed(
        sourceName, timestamp1, timestamp2, 0), 0.01);
    energyData = client.getEnergy(sourceName, timestamp1, timestamp2, 1);
    assertEquals("getEnergy on on Oscar data was wrong", 2.6633333333333336E7, energyData
        .getProperties().getPropertyAsDouble(SensorData.ENERGY_GENERATED), 0.01);
    assertEquals("getEnergy on on Oscar data was wrong", 0, energyData.getProperties()
        .getPropertyAsDouble(SensorData.ENERGY_CONSUMED), 0.01);
  }

  /**
   * Tests the energy resource on a virtual source.
   * 
   * @throws Exception If there are problems creating timestamps, or if the client has problems.
   */
  @Test
  public void testGetVirtualSourceEnergy() throws Exception {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultOwnerUsername, defaultOwnerPassword);

    XMLGregorianCalendar beforeTime, afterTime, timestamp1, timestamp2;
    SensorData beforeData, afterData;
    String source1Name = defaultPublicSource;
    String source2Name = defaultPrivateSource;
    String virtualSourceName = defaultVirtualSource;
    String source1 = Source.sourceToUri(source1Name, server);
    String source2 = Source.sourceToUri(source2Name, server);

    // timestamp = range for flat power on both sources, getEnergy should just return double
    beforeTime = Tstamp.makeTimestamp("2009-07-28T08:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source1, 100, 0, 2345, 0, false);
    client.storeSensorData(beforeData);
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source2, 100, 0, 5678, 0, false);
    client.storeSensorData(beforeData);
    afterTime = Tstamp.makeTimestamp("2009-07-28T09:00:00.000-10:00");
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source1, 100, 0, 2445, 0, false);
    client.storeSensorData(afterData);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source2, 100, 0, 5778, 0, false);
    client.storeSensorData(afterData);
    assertEquals("getEnergy on degenerate range with default interval gave wrong value", 200,
        client.getEnergyGenerated(virtualSourceName, beforeTime, afterTime, 0), 0.01);
    assertEquals("getEnergy on degenerate range with 2 minute interval gave wrong value", 200,
        client.getEnergyGenerated(virtualSourceName, beforeTime, afterTime, 2), 0.01);
    assertEquals("getEnergy on degenerate range with 5 minute interval gave wrong value", 200,
        client.getEnergyGenerated(virtualSourceName, beforeTime, afterTime, 5), 0.01);
    assertEquals("getEnergy on degenerate range with 5 minute interval gave wrong value", 200,
        client.getEnergyGenerated(virtualSourceName, beforeTime, afterTime, 30), 0.01);
    assertEquals("getEnergy on degenerate range with 5 minute interval gave wrong value", 200,
        client.getEnergyGenerated(virtualSourceName, beforeTime, afterTime, 29), 0.01);
    try {
      client.getEnergyGenerated(virtualSourceName, beforeTime, afterTime, 61);
      fail("getEnergy worked with interval longer than range");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }

    // Now try with counters
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, source1Name));
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, source2Name));
    assertEquals(
        "getEnergy on degenerate range virtual source with energy counters gave wrong value", 200,
        client.getEnergyGenerated(virtualSourceName, beforeTime, afterTime, 0), 0.01);
    assertTrue("Unable to remove energy counters", removeEnergyCounterProperty(client, source1Name));
    assertTrue("Unable to remove energy counters", removeEnergyCounterProperty(client, source2Name));

    client.deleteSensorData(source1Name, beforeData.getTimestamp());
    client.deleteSensorData(source1Name, afterData.getTimestamp());
    client.deleteSensorData(source2Name, beforeData.getTimestamp());
    client.deleteSensorData(source2Name, afterData.getTimestamp());

    // Simple, in the middle of interval
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:12:35.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source1, 1.0E7, 0, 0, 0, false);
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:13:25.000-10:00");
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source1, 2.0E7, 0, 208333.3, 0,
            false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    timestamp1 = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");

    beforeTime = Tstamp.makeTimestamp("2009-10-12T01:12:35.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source1, 1.0E7, 0, 15000000, 0,
            false);
    afterTime = Tstamp.makeTimestamp("2009-10-12T01:13:25.000-10:00");
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source1, 2.0E7, 0, 15208333.3, 0,
            false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    timestamp2 = Tstamp.makeTimestamp("2009-10-12T01:13:00.000-10:00");
    assertEquals("getEnergyGenerated on for simple gave wrong value", 1.5E7, client
        .getEnergyGenerated(source1Name, timestamp1, timestamp2, 0), 0.01);
    // Now with counters
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, source1Name));
    assertEquals("getEnergy on virtual source with energy counters gave wrong value", 1.5E7, client
        .getEnergyGenerated(source1Name, timestamp1, timestamp2, 0), 0.01);
    assertTrue("Unable to remove energy counters", removeEnergyCounterProperty(client, source1Name));

    // Computed by hand from Oscar data
    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source2, 5.5E7, 0, 5678, 0, false);
    client.storeSensorData(beforeData);
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source2, 6.4E7, 0, 14880678, 0,
            false);
    client.storeSensorData(afterData);

    beforeTime = Tstamp.makeTimestamp("2009-10-12T01:00:00.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source2, 5.5E7, 0, 59505678, 0,
            false);
    client.storeSensorData(beforeData);
    afterTime = Tstamp.makeTimestamp("2009-10-12T01:15:00.000-10:00");
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source2, 6.4E7, 0, 74409068, 0,
            false);
    client.storeSensorData(afterData);
    assertEquals("getEnergyGenerated on for simple gave wrong value", 5.948E7, client
        .getEnergyGenerated(source2Name, timestamp1, timestamp2, 0), 0.2E7);
    // Now try with counters
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, source2Name));
    assertEquals("getEnergyGenerated on for simple gave wrong value", 5.952E7, client
        .getEnergyGenerated(source2Name, timestamp1, timestamp2, 0), 0.02E7);
    assertTrue("Unable to remove energy counters", removeEnergyCounterProperty(client, source2Name));

    // Virtual source should get the sum of the two previous power values
    assertEquals("energy for virtual source did not equal expected value", 7.448E7, client
        .getEnergyGenerated(virtualSourceName, timestamp1, timestamp2, 0), 0.01);
    assertTrue("Interpolated property not found", client.getEnergy(virtualSourceName, timestamp1,
        timestamp2, 0).isInterpolated());
    // Now try with counters
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, source1Name));
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, source2Name));
    assertEquals("energy for virtual source did not equal expected value", 7.452E7, client
        .getEnergyGenerated(virtualSourceName, timestamp1, timestamp2, 0), 0.01E7);
  }

  /**
   * Tests the energy resource when energy counters roll over.
   * 
   * @throws Exception If there are problems creating timestamps, or if the client has problems.
   */
  @Test
  public void testEnergyCounterRollover() throws Exception {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultOwnerUsername, defaultOwnerPassword);

    XMLGregorianCalendar beforeTime, afterTime, timestamp1, timestamp2;
    SensorData beforeData, afterData;
    String source1Name = defaultPublicSource;
    String source2Name = defaultPrivateSource;
    String virtualSourceName = defaultVirtualSource;
    String source1 = Source.sourceToUri(source1Name, server);
    String source2 = Source.sourceToUri(source2Name, server);

    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, source1Name));
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, source2Name));

    beforeTime = Tstamp.makeTimestamp("2009-10-12T00:12:35.000-10:00");
    timestamp1 = Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T00:13:25.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source2, 123, 0, 5678, 0, false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source2, 391, 0, 6023, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source1, 724, 0, 2345, 0, false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source1, 987, 0, 2445, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);

    beforeTime = Tstamp.makeTimestamp("2009-10-12T01:12:35.000-10:00");
    timestamp2 = Tstamp.makeTimestamp("2009-10-12T01:13:00.000-10:00");
    afterTime = Tstamp.makeTimestamp("2009-10-12T01:13:25.000-10:00");
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source2, 456, 0, 27, 0, false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source2, 567, 0, 108, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);
    beforeData =
        SensorDataStraddle.makePowerEnergySensorData(beforeTime, source1, 445, 0, 2345, 0, false);
    afterData =
        SensorDataStraddle.makePowerEnergySensorData(afterTime, source1, 675, 0, 2445, 0, false);
    client.storeSensorData(beforeData);
    client.storeSensorData(afterData);

    // Should catch source2 counter roll over, though exception is kinda misleading (bad XML)
    try {
      client.getEnergyGenerated(source2Name, timestamp1, timestamp2, 0);
      fail("Got energy value despite counter rolling over");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }

    // Should catch virtual source counter roll over as well
    try {
      client.getEnergyConsumed(virtualSourceName, timestamp1, timestamp2, 0);
      fail("Got energy value despite counter rolling over");
    }
    catch (BadXmlException e) { // NOPMD
      // Expected in this case
    }
  }

  /**
   * Tests a problem reported via the HiREAP project: energy retrieval between two sensordata
   * resources.
   * 
   * @throws Exception If there are problems creating timestamps, or if the client has problems.
   */
  @Test
  public void testHneiEnergyIssue() throws Exception {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultOwnerUsername, defaultOwnerPassword);
    XMLGregorianCalendar startDataTstamp = Tstamp.makeTimestamp("1999-08-01T06:00:00.000-10:00");
    XMLGregorianCalendar endDataTstamp = Tstamp.makeTimestamp("1999-08-11T06:00:00.000-10:00");

    XMLGregorianCalendar energyStart = Tstamp.makeTimestamp("1999-08-05T06:00:00.000-10:00");
    XMLGregorianCalendar energyEnd = Tstamp.makeTimestamp("1999-08-06T06:00:00.000-10:00");

    String sourceName = defaultPublicSource;
    String sourceURI = Source.sourceToUri(sourceName, getHostName());
    SensorData startData, endData, energyData;

    // Add energy counter property to source
    assertTrue("Unable to add energy counters", addEnergyCounterProperty(client, sourceName));

    // Create SensorData objects
    startData =
        SensorDataStraddle.makePowerEnergySensorData(startDataTstamp, sourceURI, 0, 0, 0, 3000000,
            false);
    client.storeSensorData(startData);
    endData =
        SensorDataStraddle.makePowerEnergySensorData(endDataTstamp, sourceURI, 0, 0, 0, 6000000,
            false);
    client.storeSensorData(endData);

    // Retrieve calculated energy data
    energyData = client.getEnergy(sourceName, energyStart, energyEnd, 0);

    assertEquals("Unexpected energy value returned by WattDepot", 300000.0, energyData
        .getPropertyAsDouble(SensorData.ENERGY_CONSUMED), 0.0001);
  }

  /**
   * Adds the supportsEnergyCounters property to the source with the provided name.
   * 
   * @param client The client instance to use for the modification.
   * @param sourceName The name of the Source to be modified.
   * @return True if the property could be added, false otherwise.
   * @throws NotAuthorizedException If there are client problems.
   * @throws ResourceNotFoundException If there are client problems.
   * @throws BadXmlException If there are client problems.
   * @throws MiscClientException If there are client problems.
   * @throws OverwriteAttemptedException Should never be thrown, since we are always overwriting.
   * @throws JAXBException If there are XML problems.
   */
  public static boolean addEnergyCounterProperty(WattDepotClient client, String sourceName)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException, OverwriteAttemptedException, JAXBException {
    Source source = client.getSource(sourceName);
    source.addProperty(new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true"));
    return client.storeSource(source, true);
  }

  /**
   * Removes the supportsEnergyCounters property to the source with the provided name.
   * 
   * @param client The client instance to use for the modification.
   * @param sourceName The name of the Source to be modified.
   * @return True if the property could be removed, false otherwise.
   * @throws NotAuthorizedException If there are client problems.
   * @throws ResourceNotFoundException If there are client problems.
   * @throws BadXmlException If there are client problems.
   * @throws MiscClientException If there are client problems.
   * @throws OverwriteAttemptedException Should never be thrown, since we are always overwriting.
   * @throws JAXBException If there are XML problems.
   */
  public static boolean removeEnergyCounterProperty(WattDepotClient client, String sourceName)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException, OverwriteAttemptedException, JAXBException {
    Source source = client.getSource(sourceName);
    Property counterProp = new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true");
    return source.getProperties().getProperty().remove(counterProp)
        && client.storeSource(source, true);
  }

}
