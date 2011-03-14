package org.wattdepot.server.db.berkeleydb;

import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.StraddleList;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDataIndex;
import org.wattdepot.resource.sensordata.jaxb.SensorDatas;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SourceIndex;
import org.wattdepot.resource.source.jaxb.Sources;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.resource.user.jaxb.UserIndex;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbBadIntervalException;
import org.wattdepot.server.db.DbImplementation;

/**
 * WattDepot DbImplementation using BerkeleyDB as the data store. This is an alternative
 * implementation to be used to compare against other DbImplementations.
 * 
 * @author George Lee
 * 
 */
public class BerkeleyDbImplementation extends DbImplementation {

  public BerkeleyDbImplementation(Server server) {
    super(server);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean deleteSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean deleteSensorData(String sourceName) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean deleteSource(String sourceName) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean deleteUser(String username) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  protected SensorData getLatestNonVirtualSensorData(String sourceName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SensorData getSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SensorDataIndex getSensorDataIndex(String sourceName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SensorDataIndex getSensorDataIndex(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws DbBadIntervalException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SensorDataStraddle getSensorDataStraddle(String sourceName, XMLGregorianCalendar timestamp) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SensorDataStraddle> getSensorDataStraddleList(String sourceName,
      XMLGregorianCalendar timestamp) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<List<SensorDataStraddle>> getSensorDataStraddleListOfLists(String sourceName,
      List<XMLGregorianCalendar> timestampList) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SensorDatas getSensorDatas(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws DbBadIntervalException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Source getSource(String sourceName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SourceIndex getSourceIndex() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SourceSummary getSourceSummary(String sourceName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Sources getSources() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<StraddleList> getStraddleLists(String sourceName,
      List<XMLGregorianCalendar> timestampList) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public User getUser(String username) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public UserIndex getUsers() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean indexTables() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void initialize(boolean wipe) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isFreshlyCreated() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean makeSnapshot() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean performMaintenance() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storeSensorData(SensorData data) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storeSource(Source source, boolean overwrite) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storeUser(User user) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean wipeData() {
    // TODO Auto-generated method stub
    return false;
  }

}
