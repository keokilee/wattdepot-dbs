package org.wattdepot.server.db.mongodb;

import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
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
import org.wattdepot.util.StackTrace;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class MongoDbImplementation extends DbImplementation {
  private static final String UNABLE_TO_PARSE_PROPERTY_XML =
    "Unable to parse property XML from database ";

  private boolean isFreshlyCreated;
  private DBCollection sensorDataCollection;
  private DBCollection sourceCollection;
  private DBCollection userCollection;
  
  /** Property JAXBContext. */
  private static final JAXBContext propertiesJAXB;
  
  static {
    try {
      propertiesJAXB =
          JAXBContext.newInstance(org.wattdepot.resource.property.jaxb.Properties.class);
    }
    catch (Exception e) {
      throw new RuntimeException("Couldn't create JAXB context instance.", e);
    }
  }

  public MongoDbImplementation(Server server) {
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
    String mongoServer = "localhost";
    Integer mongoPort = 27017;
    Mongo mongo;
    try {
      mongo = new Mongo(mongoServer, mongoPort);
    }
    catch (UnknownHostException e) {
      throw new RuntimeException("Could not connect to " + mongoServer);
    }
    catch (MongoException e) {
      throw new RuntimeException("Could not connect to "  + mongoServer + "on port " + mongoPort.toString());
    }
    
    //Check if the database exists.
    String mongoDbName = "wattdepot";
    List<String> mongoDbs = mongo.getDatabaseNames();
    this.isFreshlyCreated = !mongoDbs.contains(mongoDbName);
    DB mongoDb = mongo.getDB(mongoDbName);
    
    //Set up collections and indices.
    this.sensorDataCollection = mongoDb.getCollection("sensorData");
    BasicDBObject dataIndex = new BasicDBObject();
    dataIndex.put("source", 1);
    dataIndex.put("timestamp", 1);
    BasicDBObject uniqueOption = new BasicDBObject("unique", true);
    this.sensorDataCollection.ensureIndex(dataIndex, uniqueOption);
    
    this.sourceCollection = mongoDb.getCollection("sources");
    this.sourceCollection.ensureIndex(new BasicDBObject("name", 1), uniqueOption);
    
    this.userCollection = mongoDb.getCollection("users");
    this.userCollection.ensureIndex(new BasicDBObject("name", 1), uniqueOption);
  }

  @Override
  public boolean isFreshlyCreated() {
    return this.isFreshlyCreated;
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
    BasicDBObject dbData = new BasicDBObject();
    dbData.put("source", data.getSource());
    dbData.put("timestamp",data.getTimestamp().toGregorianCalendar().getTimeInMillis());
    dbData.put("tool", data.getTool());
    
    if (data.isSetProperties()) {
      try {
        Marshaller propertiesMarshaller = propertiesJAXB.createMarshaller();
        StringWriter writer = new StringWriter();
        propertiesMarshaller.marshal(data.getProperties(), writer);
        dbData.put("properties", writer.toString());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    WriteResult result = this.sensorDataCollection.insert(dbData);
    return result.getError() == null;
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
