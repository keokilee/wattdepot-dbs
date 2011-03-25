package org.wattdepot.server.db.mongodb;

import java.io.StringReader;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.StraddleList;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDataIndex;
import org.wattdepot.resource.sensordata.jaxb.SensorDataRef;
import org.wattdepot.resource.sensordata.jaxb.SensorDatas;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SourceIndex;
import org.wattdepot.resource.source.jaxb.SourceRef;
import org.wattdepot.resource.source.jaxb.Sources;
import org.wattdepot.resource.source.jaxb.SubSources;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.resource.user.jaxb.UserIndex;
import org.wattdepot.resource.user.jaxb.UserRef;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbBadIntervalException;
import org.wattdepot.server.db.DbImplementation;
import org.wattdepot.util.StackTrace;
import org.wattdepot.util.tstamp.Tstamp;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class MongoDbImplementation extends DbImplementation {
  private static final String UNABLE_TO_PARSE_PROPERTY_XML =
    "Unable to parse property XML from database ";

  private static JAXBContext subSourcesJAXB;

  private boolean isFreshlyCreated;
  private DBCollection sensorDataCollection;
  private DBCollection sourceCollection;
  private DBCollection userCollection;
  
  /** Property JAXBContext. */
  private static final JAXBContext propertiesJAXB;
  
  static {
    try {
      propertiesJAXB = JAXBContext.newInstance(org.wattdepot.resource.property.jaxb.Properties.class);
      subSourcesJAXB = JAXBContext.newInstance(org.wattdepot.resource.source.jaxb.SubSources.class);
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
    if (sourceName == null || timestamp == null) {
      return false;
    }
    
    String sourceUri = Source.sourceToUri(sourceName, this.server);
    BasicDBObject query = new BasicDBObject("source", sourceUri);
    query.put("timestamp", timestamp.toGregorianCalendar().getTimeInMillis());
    
    DBObject object = this.sensorDataCollection.findAndRemove(query);
    
    return object != null;
  }

  @Override
  public boolean deleteSensorData(String sourceName) {
    if (sourceName == null) {
      return false;
    }
    
    String sourceUri = Source.sourceToUri(sourceName, this.server);
    BasicDBObject query = new BasicDBObject("source", sourceUri);
    WriteResult result = this.sensorDataCollection.remove(query);
    return result.getError() == null;
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

  private SensorData dbObjectToSensorData(DBObject object) {
    SensorData data = new SensorData();
    data.setSource((String) object.get("source"));
    data.setTimestamp(Tstamp.makeTimestamp((Long)object.get("timestamp")));
    data.setTool((String) object.get("tool"));
    
    if (object.containsField("properties")) {
      String props = (String)object.get("properties");
      try {
        Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
        data.setProperties((Properties) unmarshaller.unmarshal(new StringReader(props)));
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    return data;
  }
  
  @Override
  public SensorData getSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    BasicDBObject query = new BasicDBObject();
    query.put("source", Source.sourceToUri(sourceName, this.server));
    query.put("timestamp", timestamp.toGregorianCalendar().getTimeInMillis());
    DBObject object = this.sensorDataCollection.findOne(query);
    
    if (object == null) {
      return null;
    }
    
    return this.dbObjectToSensorData(object);
  }

  @Override
  public SensorDataIndex getSensorDataIndex(String sourceName) {
    try {
      return this.getSensorDataIndex(sourceName, Tstamp.makeTimestamp(0), Tstamp.makeTimestamp());
    }
    catch (DbBadIntervalException e) {
      // Should not happen
      this.logger.warning("getSensorDataIndex failed for an interval from 0 to today.");
      return null;
    }
  }

  @Override
  public SensorDataIndex getSensorDataIndex(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws DbBadIntervalException {
    if (sourceName == null || startTime == null || endTime == null) {
      return null;
    }
    else if (this.getSource(sourceName) == null) {
      // Unknown Source name, therefore no possibility of SensorData
      return null;
    }
    else if (startTime.compare(endTime) == DatatypeConstants.GREATER) {
      // startTime > endTime, which is bogus
      throw new DbBadIntervalException(startTime, endTime);
    }
    
    //Construct the query
    SensorDataIndex index = new SensorDataIndex();
    String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
    Long start = startTime.toGregorianCalendar().getTimeInMillis();
    Long end = endTime.toGregorianCalendar().getTimeInMillis();
    
    BasicDBObject query = new BasicDBObject();
    query.put("source", sourceUri);
    // Mongo uses "gte" and "lte" for greater/less than or equal
    query.put("timestamp", new BasicDBObject("$gte", start).append("$lte", end));
    
    DBCursor cursor = this.sensorDataCollection.find(query);
    SensorDataRef ref;
    for (DBObject object : cursor) {
      ref = new SensorDataRef(Tstamp.makeTimestamp((Long)object.get("timestamp")), 
          (String) object.get("tool"), (String) object.get("source"));
      index.getSensorDataRef().add(ref);
    }
    return index;
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

  private Source dbObjectToSource(DBObject object) {
    Source source = new Source();
    source.setName((String)object.get("name"));
    source.setOwner((String)object.get("owner"));
    source.setPublic((Boolean)object.get("isPublic"));
    source.setVirtual((Boolean)object.get("isVirtual"));
    source.setLocation((String)object.get("location"));
    source.setCoordinates((String)object.get("coordinates"));
    if (object.containsField("properties")) {
      String props = (String)object.get("properties");
      try {
        Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
        source.setProperties((Properties) unmarshaller.unmarshal(new StringReader(props)));
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    if (object.containsField("subSources")) {
      String subSources = (String)object.get("subSources");
      try {
        Unmarshaller unmarshaller = subSourcesJAXB.createUnmarshaller();
        source.setSubSources((SubSources) unmarshaller.unmarshal(new StringReader(subSources)));
      }
      catch (JAXBException e) {
        this.logger.warning("Unable to parse SubSource XML " + StackTrace.toString(e));
      }
    }
    
    return source;
  }
  
  private SourceRef dbObjectToSourceRef(DBObject object) {
    SourceRef ref = new SourceRef();
    ref.setName((String) object.get("name"));
    ref.setOwner((String) object.get("owner"));
    ref.setCoordinates((String) object.get("coordinates"));
    ref.setDescription((String)object.get("description"));
    ref.setLocation((String) object.get("location"));
    ref.setPublic((Boolean) object.get("isPublic"));
    ref.setVirtual((Boolean) object.get("isVirtual"));
    ref.setHref((String) object.get("name"), this.server);
    return ref;
  }
  
  @Override
  public Source getSource(String sourceName) {
    if (sourceName == null) {
      return null;
    }
    
    BasicDBObject query = new BasicDBObject("source", Source.sourceToUri(sourceName, this.server));
    DBObject dbSource = this.sourceCollection.findOne(query);
    if (dbSource == null) {
      return null;
    }
    
    return this.dbObjectToSource(dbSource);
  }

  @Override
  public SourceIndex getSourceIndex() {
    SourceIndex index = new SourceIndex();
    DBCursor cursor = this.sourceCollection.find();
    for (DBObject object : cursor) {
      index.getSourceRef().add(this.dbObjectToSourceRef(object));
    }
    cursor.close();
    return index;
  }

  @Override
  public SourceSummary getSourceSummary(String sourceName) {
    if (sourceName == null) {
      // null or non-existent source name
      return null;
    }
    Source baseSource = getSource(sourceName);
    if (baseSource == null) {
      return null;
    }
    
    SourceSummary summary = new SourceSummary();
    summary.setHref(Source.sourceToUri(sourceName, this.server.getHostName()));
    
    // Want to go through sensordata for base source, and all subsources recursively
    List<Source> sourceList = getAllNonVirtualSubSources(baseSource);
    Long firstTimestamp = null, lastTimestamp = null;
    DBCursor cursor;
    DBObject temp;
    List<DBObject> sensorData;
    int dataCount = 0;
    String subsourceUri;
    for (Source subSource : sourceList) {
      subsourceUri = Source.sourceToUri(subSource.getName(), this.server.getHostName());
      //Create cursor for getting data.
      cursor = this.sensorDataCollection.find(new BasicDBObject("source", subsourceUri));
      sensorData = cursor.toArray();
      
      //Count the number of results for this source.
      dataCount += sensorData.size();
      if (dataCount > 0) {
        //Get first timestamp of sensor data.
        temp = sensorData.get(0);
        if (firstTimestamp == null || (Long)temp.get("timestamp") < firstTimestamp) {
          firstTimestamp = (Long)temp.get("timestamp");
        }
        
        //Get last timestamp of sensor data.
        temp = sensorData.get(sensorData.size() - 1);
        if (lastTimestamp == null || (Long)temp.get("timestamp") > lastTimestamp) {
          lastTimestamp = (Long)temp.get("timestamp");
        }
      }
      //Clean up
      cursor.close();
    }
    
    summary.setFirstSensorData(Tstamp.makeTimestamp(firstTimestamp));
    summary.setLastSensorData(Tstamp.makeTimestamp(lastTimestamp));
    summary.setTotalSensorDatas(dataCount);
    return summary;
  }

  @Override
  public Sources getSources() {
    Sources sources = new Sources();
    DBCursor cursor = this.sourceCollection.find();
    for (DBObject object : cursor) {
      sources.getSource().add(this.dbObjectToSource(object));
    }
    
    cursor.close();
    return sources;
  }

  @Override
  public List<StraddleList> getStraddleLists(String sourceName,
      List<XMLGregorianCalendar> timestampList) {
    // TODO Auto-generated method stub
    return null;
  }

  private User dbObjectToUser(DBObject object) {
    User user = new User();
    user.setEmail((String) object.get("name"));
    user.setPassword((String) object.get("password"));
    user.setAdmin((Boolean) object.get("isAdmin"));
    
    if (object.containsField("properties")) {
      String props = (String)object.get("properties");
      try {
        Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
        user.setProperties((Properties) unmarshaller.unmarshal(new StringReader(props)));
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    return user;
  }
  
  @Override
  public User getUser(String username) {
    if (username == null) {
      return null;
    }
    
    BasicDBObject query = new BasicDBObject("name", username);
    DBObject object = this.userCollection.findOne(query);
    if (object == null) {
      return null;
    }
    
    return this.dbObjectToUser(object);
  }

  @Override
  public UserIndex getUsers() {
    UserIndex index = new UserIndex();
    DBCursor cursor = this.userCollection.find();
    for (DBObject object : cursor) {
      index.getUserRef().add(new UserRef((String) object.get("name"), this.server));
    }
    cursor.close();
    return index;
  }

  @Override
  public boolean hasSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    return this.getSensorData(sourceName, timestamp) != null;
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
    dbData.put("lastMod", System.currentTimeMillis());
    
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
    BasicDBObject dbSource = new BasicDBObject();
    dbSource.put("name", source.getName());
    dbSource.put("owner", source.getOwner());
    dbSource.put("location", source.getLocation());
    dbSource.put("coordinates", source.getCoordinates());
    dbSource.put("isPublic", source.isPublic());
    dbSource.put("isVirtual", source.isVirtual());
    dbSource.put("lastMod", System.currentTimeMillis());
    
    if (source.isSetProperties()) {
      try {
        Marshaller propertiesMarshaller = propertiesJAXB.createMarshaller();
        StringWriter writer = new StringWriter();
        propertiesMarshaller.marshal(source.getProperties(), writer);
        dbSource.put("properties", writer.toString());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    if (source.isSetSubSources()) {
      try {
        Marshaller subSourcesMarshaller = subSourcesJAXB.createMarshaller();
        StringWriter writer = new StringWriter();
        subSourcesMarshaller.marshal(source.getSubSources(), writer);
        dbSource.put("subSources", writer.toString());
      }
      catch (JAXBException e) {
        this.logger.warning("Unable to parse subsources " + StackTrace.toString(e));
      }
    }
    
    WriteResult result;
    if (overwrite) {
      //Find an existing source with this name.
      BasicDBObject query = new BasicDBObject("name", source.getName());
      //Third parameter is to "upsert" (update if exists, insert otherwise).
      //Fourth is to update all matching objects, though there should be only one.
      result = this.sourceCollection.update(query, dbSource, true, true);
    }
    else {
      result = this.sourceCollection.insert(dbSource);
    }
    
    return result.getError() == null;
  }

  @Override
  public boolean storeUser(User user) {
    BasicDBObject dbUser = new BasicDBObject();
    dbUser.put("name", user.getEmail());
    dbUser.put("password", user.getPassword());
    dbUser.put("isAdmin", user.isAdmin());
    dbUser.put("lastMod", System.currentTimeMillis());
    
    if (user.isSetProperties()) {
      try {
        Marshaller propertiesMarshaller = propertiesJAXB.createMarshaller();
        StringWriter writer = new StringWriter();
        propertiesMarshaller.marshal(user.getProperties(), writer);
        dbUser.put("properties", writer.toString());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    WriteResult result = this.userCollection.insert(dbUser);
    if (result.getError() != null) {
      this.logger.warning("MongoDB: Received error while trying to insert user " + result.getError());
    }
    
    return result.getError() == null;
  }

  @Override
  public boolean wipeData() {
    // TODO Auto-generated method stub
    return false;
  }

}
