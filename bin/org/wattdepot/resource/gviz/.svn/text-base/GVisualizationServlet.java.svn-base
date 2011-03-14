package org.wattdepot.resource.gviz;

import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDataIndex;
import org.wattdepot.resource.sensordata.jaxb.SensorDataRef;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbBadIntervalException;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.util.tstamp.Tstamp;
import com.google.common.collect.Lists;
import com.google.visualization.datasource.Capabilities;
import com.google.visualization.datasource.DataSourceServlet;
import com.google.visualization.datasource.base.DataSourceException;
import com.google.visualization.datasource.base.ReasonType;
import com.google.visualization.datasource.base.TypeMismatchException;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.TableRow;
import com.google.visualization.datasource.datatable.value.DateTimeValue;
import com.google.visualization.datasource.datatable.value.ValueType;
import com.google.visualization.datasource.query.AbstractColumn;
import com.google.visualization.datasource.query.Query;
import com.ibm.icu.util.TimeZone;

/**
 * Accepts requests using the Google Visualization API and fulfills then with data from the
 * WattDepot database. Currently does not use authentication, and therefore does not obey the
 * WattDepot security model.
 * 
 * Portions of this class were derived from the AdvancedExampleServlet example class from the Google
 * Visualization data source library, which is distributed under the Apache License, Version 2.0.
 * See http://www.apache.org/licenses/LICENSE-2.0 for full text of the Apache license.
 * 
 * @author Robert Brewer
 */
public class GVisualizationServlet extends DataSourceServlet {

  /** Keep Eclipse happy. */
  private static final long serialVersionUID = 1L;

  /** The WattDepot server (not Servlet container) for contact with WattDepot database. */
  protected transient Server server;

  /** The WattDepot database manager, for retrieving sensor data. */
  protected transient DbManager dbManager;

  /** Conversion factor for milliseconds per minute. */
  private static final long MILLISECONDS_PER_MINUTE = 60L * 1000;

  /** Name for timestamp column of data table. */
  private static final String TIME_POINT_COLUMN = "timePoint";
  /** Name for power consumed column of data table. */
  private static final String POWER_CONSUMED_COLUMN = SensorData.POWER_CONSUMED;
  /** Name for power consumed column of data table. */
  private static final String POWER_GENERATED_COLUMN = SensorData.POWER_GENERATED;
  /** Name for energy consumed to date (a counter) column of data table. */
  private static final String ENERGY_CONSUMED_TO_DATE_COLUMN = SensorData.ENERGY_CONSUMED_TO_DATE;
  /** Name for energy consumed to date (a counter) column of data table. */
  private static final String ENERGY_GENERATED_TO_DATE_COLUMN = SensorData.ENERGY_GENERATED_TO_DATE;

  /** List of all possible columns for sensor data requests. */
  private static final ColumnDescription[] SENSOR_DATA_TABLE_COLUMNS =
      new ColumnDescription[] {
          new ColumnDescription(TIME_POINT_COLUMN, ValueType.DATETIME, "Date & Time"),
          new ColumnDescription(POWER_CONSUMED_COLUMN, ValueType.NUMBER, "Power Cons. (W)"),
          new ColumnDescription(POWER_GENERATED_COLUMN, ValueType.NUMBER, "Power Gen. (W)"),
          new ColumnDescription(ENERGY_CONSUMED_TO_DATE_COLUMN, ValueType.NUMBER,
              "Energy Consumed To Date (Wh)"),
          new ColumnDescription(ENERGY_GENERATED_TO_DATE_COLUMN, ValueType.NUMBER,
              "Energy Generated To Date (Wh)") };

  /** Name for energy consumed column of data table. */
  private static final String ENERGY_CONSUMED_COLUMN = "energyConsumed";
  /** Name for energy generated column of data table. */
  private static final String ENERGY_GENERATED_COLUMN = "energyGenerated";
  /** Name for carbon emitted column of data table. */
  private static final String CARBON_EMITTED_COLUMN = "carbonEmitted";

  /** List of all possible columns for calculated value requests. */
  private static final ColumnDescription[] CALCULATED_TABLE_COLUMNS =
      new ColumnDescription[] {
          new ColumnDescription(TIME_POINT_COLUMN, ValueType.DATETIME, "Date & Time"),
          new ColumnDescription(POWER_CONSUMED_COLUMN, ValueType.NUMBER, "Power Cons. (W)"),
          new ColumnDescription(POWER_GENERATED_COLUMN, ValueType.NUMBER, "Power Gen. (W)"),
          new ColumnDescription(ENERGY_CONSUMED_COLUMN, ValueType.NUMBER, "Energy Cons. (Wh)"),
          new ColumnDescription(ENERGY_GENERATED_COLUMN, ValueType.NUMBER, "Energy Gen. (Wh)"),
          new ColumnDescription(CARBON_EMITTED_COLUMN, ValueType.NUMBER, "Carbon (lbs CO2)") };

  /**
   * Create the new servlet, and record the provided Server to make queries on the DB.
   * 
   * @param server The WattDepot server this servlet shares data with.
   */
  public GVisualizationServlet(Server server) {
    super();
    this.server = server;
    this.dbManager = (DbManager) server.getContext().getAttributes().get("DbManager");
  }

  /** {@inheritDoc} */
  public DataTable generateDataTable(Query query, HttpServletRequest request)
      throws DataSourceException {
    String remainingUri, sourceName;
    boolean sensorDataRequested = false, latestSensorDataRequested = false;

//    System.out.println("Request: " + request.getRequestURL() + "?" + request.getQueryString()); // DEBUG

    // This is everything following the URI, which should start with "/gviz/source/"
    String rawPath = request.getPathInfo();
//    System.out.println("rawPath: " + rawPath); // DEBUG
    // Check for incomplete URIs
    if ((rawPath == null) || ("".equals(rawPath)) || "/".equals(rawPath)) {
      throw new DataSourceException(ReasonType.INVALID_REQUEST, "No Source name provided.");
    }
    else {
      if ((rawPath.length() > 1) && (rawPath.startsWith("/"))) {
        // need to strip off leading "/" to get remaining URI
        remainingUri = rawPath.substring(1);
      }
      else {
        // this shouldn't happen, based on my understanding of the path provided to the servlet
        throw new DataSourceException(ReasonType.INTERNAL_ERROR,
            "Internal problem with Source name provided.");
      }
//      System.out.println("remaningUri: " + remainingUri); // DEBUG

      // remainingUri should look like "SOURCE_NAME/sensordata" or "SOURCE_NAME/calculated"
      // Slash separates source name from data type requested
      int slashIndex = remainingUri.indexOf('/');
      if (slashIndex == -1) {
        // No slash found, bad request
        throw new DataSourceException(ReasonType.INVALID_REQUEST,
            "Bad source name or data type parameter provided: " + remainingUri);
      }
      else {
        sourceName = remainingUri.substring(0, slashIndex);
        // TODO Should filter the source name, check for unexpected characters and length
        // Check if the given source name exists in database
        if (dbManager.getSource(sourceName) == null) {
          throw new DataSourceException(ReasonType.INVALID_REQUEST, "No Source named \""
              + sourceName + "\".");
        }
      }

//      System.out.println(sourceName); // DEBUG

      String dataTypeRequested = remainingUri.substring(slashIndex + 1);
      if ("sensordata".equals(dataTypeRequested)) {
        sensorDataRequested = true;
      }
      else if ("sensordata/latest".equals(dataTypeRequested)) {
        latestSensorDataRequested = true;
      }
      // Otherwise, just grab the source name
      else if ("calculated".equals(dataTypeRequested)) {
        sensorDataRequested = false;
      }
      else {
        throw new DataSourceException(ReasonType.INVALID_REQUEST, "Invalid data type requested: "
            + dataTypeRequested);
      }
    }

    String startTimeString = getQueryParameter(request, "startTime");
    String endTimeString = getQueryParameter(request, "endTime");
    XMLGregorianCalendar startTime = null;
    XMLGregorianCalendar endTime = null;
    if ((startTimeString != null) && (endTimeString != null)) {
      try {
        startTime = Tstamp.makeTimestamp(startTimeString);
      }
      catch (Exception e) { // NOPMD
        log("Unable to convert startTime parameter to XMLGregorianCalendar", e);
        throw new DataSourceException(ReasonType.INVALID_REQUEST, // NOPMD
            "startTime parameter was invalid."); // NOPMD
      }
      try {
        endTime = Tstamp.makeTimestamp(endTimeString);
      }
      catch (Exception e) {
        log("Unable to convert endTime parameter to XMLGregorianCalendar", e);
        throw new DataSourceException(ReasonType.INVALID_REQUEST, "endTime parameter was invalid."); // NOPMD
      }
    }

    if (sensorDataRequested) {
      return generateSensorDataTable(query, sourceName, startTime, endTime);
    }
    else if (latestSensorDataRequested) {
      return generateLatestSensorDataTable(query, sourceName);
    }
    // Calculated data: power, energy, carbon
    else {
      String intervalString = getQueryParameter(request, "samplingInterval");
      int intervalMinutes = 0;

      if (intervalString != null) {
        // convert to integer
        try {
          intervalMinutes = Integer.valueOf(intervalString);
        }
        catch (NumberFormatException e) {
          log("Unable to convert samplingInterval parameter to int", e);
          throw new DataSourceException(ReasonType.INVALID_REQUEST, // NOPMD
              "samplingInterval parameter was invalid."); // NOPMD
        }
      }
      String displaySubsourcesString = getQueryParameter(request, "displaySubsources");
      boolean displaySubsources = "true".equals(displaySubsourcesString);
      return generateCalculatedTable(query, sourceName, startTime, endTime, intervalMinutes,
          displaySubsources);
    }
  }

  /**
   * Generates a DataTable of sensor data, given the query parameters. Supports the SELECT
   * capability, so only columns that are SELECTed will be retrieved and added to the table. The
   * startTime and endTime parameters can be null, in which case all sensor data for the source will
   * be retrieved (which can be very big).
   * 
   * @param query The query from the data source client.
   * @param sourceName The name of the source.
   * @param startTime The starting time for the interval.
   * @param endTime The ending time for the interval.
   * @return A DataTable with the selected columns for every sensor data resource within the
   * interval.
   * @throws DataSourceException If there are problems fulfilling the request.
   */
  private DataTable generateSensorDataTable(Query query, String sourceName,
      XMLGregorianCalendar startTime, XMLGregorianCalendar endTime) throws DataSourceException {
    DataTable data = new DataTable();
    // Sets up the columns requested by any SELECT in the data source query
    List<ColumnDescription> requiredColumns =
        getRequiredColumns(query, SENSOR_DATA_TABLE_COLUMNS, null);
    data.addColumns(requiredColumns);

    SensorDataIndex index;
    // Get all sensor data for this Source
    if ((startTime == null) || (endTime == null)) {
      index = dbManager.getSensorDataIndex(sourceName);
    }
    // If we received valid start and end times, retrieve just that data
    else {
      try {
        index = dbManager.getSensorDataIndex(sourceName, startTime, endTime);
      }
      catch (DbBadIntervalException e) {
        log("startTime came after endTime", e);
        throw new DataSourceException(ReasonType.INVALID_REQUEST, // NOPMD
            "startTime parameter was after endTime parameter."); // NOPMD
      }
    }
    // Iterate over each SensorDataRef
    for (SensorDataRef ref : index.getSensorDataRef()) {
      SensorData sensorData = dbManager.getSensorData(sourceName, ref.getTimestamp());
      TableRow row = new TableRow();
      for (ColumnDescription selectionColumn : requiredColumns) {
        String columnName = selectionColumn.getId();
        try {
          if (columnName.equals(TIME_POINT_COLUMN)) {
            row.addCell(new DateTimeValue(convertTimestamp(ref.getTimestamp())));
          }
          else if (columnName.equals(POWER_CONSUMED_COLUMN)) {
            row.addCell(sensorData.getPropertyAsDouble(SensorData.POWER_CONSUMED));
          }
          else if (columnName.equals(POWER_GENERATED_COLUMN)) {
            row.addCell(sensorData.getPropertyAsDouble(SensorData.POWER_GENERATED));
          }
          else if (columnName.equals(ENERGY_CONSUMED_TO_DATE_COLUMN)) {
            row.addCell(sensorData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE));
          }
          else if (columnName.equals(ENERGY_GENERATED_TO_DATE_COLUMN)) {
            row.addCell(sensorData.getPropertyAsDouble(SensorData.ENERGY_GENERATED_TO_DATE));
          }
        }
        catch (NumberFormatException e) {
          // String value in database couldn't be converted to a number.
          throw new DataSourceException(ReasonType.INTERNAL_ERROR, "Found bad number in database"); // NOPMD
        }
      }
      try {
        data.addRow(row);
      }
      catch (TypeMismatchException e) {
        throw new DataSourceException(ReasonType.INTERNAL_ERROR, "Problem adding data to table"); // NOPMD
      }
    }
    return data;
  }

  /**
   * Generates a DataTable of the latest sensor data (which will have a single row), given the query
   * parameters. Supports the SELECT capability, so only columns that are SELECTed will be retrieved
   * and added to the table.
   * 
   * @param query The query from the data source client.
   * @param sourceName The name of the source.
   * @return A DataTable with the selected columns for every sensor data resource within the
   * interval.
   * @throws DataSourceException If there are problems fulfilling the request.
   */
  private DataTable generateLatestSensorDataTable(Query query, String sourceName)
      throws DataSourceException {
    DataTable data = new DataTable();
    // Sets up the columns requested by any SELECT in the data source query
    List<ColumnDescription> requiredColumns =
        getRequiredColumns(query, SENSOR_DATA_TABLE_COLUMNS, null);
    data.addColumns(requiredColumns);

    SensorData sensorData = dbManager.getLatestSensorData(sourceName);
    TableRow row = new TableRow();
    for (ColumnDescription selectionColumn : requiredColumns) {
      String columnName = selectionColumn.getId();
      try {
        if (columnName.equals(TIME_POINT_COLUMN)) {
          row.addCell(new DateTimeValue(convertTimestamp(sensorData.getTimestamp())));
        }
        else if (columnName.equals(POWER_CONSUMED_COLUMN)) {
          row.addCell(sensorData.getPropertyAsDouble(SensorData.POWER_CONSUMED));
        }
        else if (columnName.equals(POWER_GENERATED_COLUMN)) {
          row.addCell(sensorData.getPropertyAsDouble(SensorData.POWER_GENERATED));
        }
        else if (columnName.equals(ENERGY_CONSUMED_TO_DATE_COLUMN)) {
          row.addCell(sensorData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE));
        }
        else if (columnName.equals(ENERGY_GENERATED_TO_DATE_COLUMN)) {
          row.addCell(sensorData.getPropertyAsDouble(SensorData.ENERGY_GENERATED_TO_DATE));
        }
      }
      catch (NumberFormatException e) {
        // String value in database couldn't be converted to a number.
        throw new DataSourceException(ReasonType.INTERNAL_ERROR, "Found bad number in database"); // NOPMD
      }
    }
    try {
      data.addRow(row);
    }
    catch (TypeMismatchException e) {
      throw new DataSourceException(ReasonType.INTERNAL_ERROR, "Problem adding data to table"); // NOPMD
    }
    return data;
  }

  /**
   * Generates a DataTable of calculated data, given the query parameters. Supports the SELECT
   * capability, so only columns that are SELECTed will be retrieved and added to the table. The
   * startTime and endTime parameters are required, as is the sampling interval.
   * 
   * @param query The query from the data source client.
   * @param sourceName The name of the source.
   * @param startTime The starting time for the interval.
   * @param endTime The ending time for the interval.
   * @param intervalMinutes the rate at which the selected columns should be sampled, in minutes.
   * @param displaySubsources True if subsources are to be included as additional columns in the
   * DataTable, false otherwise.
   * @return A DataTable with the selected columns sampled at the given rate within the interval.
   * @throws DataSourceException If there are problems fulfilling the request.
   */
  private DataTable generateCalculatedTable(Query query, String sourceName,
      XMLGregorianCalendar startTime, XMLGregorianCalendar endTime, int intervalMinutes,
      boolean displaySubsources) throws DataSourceException {
    DataTable data = new DataTable();

    if ((startTime == null) || (endTime == null)) {
      throw new DataSourceException(ReasonType.INVALID_REQUEST,
          "Valid startTime and endTime parameters are required.");
    }
    else if (Tstamp.greaterThan(startTime, endTime)) {
      throw new DataSourceException(ReasonType.INVALID_REQUEST,
          "startTime parameter later than endTime parameter");
    }

    SensorData powerData = null, energyData = null, carbonData = null;
    long intervalMilliseconds;
    long rangeLength = Tstamp.diff(startTime, endTime);
    long minutesToMilliseconds = 60L * 1000L;

    if (intervalMinutes < 0) {
      log("samplingInterval parameter less than 0");
      throw new DataSourceException(ReasonType.INVALID_REQUEST,
          "samplingInterval parameter was less than 0.");
    }
    else if (intervalMinutes == 0) {
      // use default interval
      intervalMilliseconds = rangeLength / 10;
    }
    else if ((intervalMinutes * minutesToMilliseconds) > rangeLength) {
      log("samplingInterval parameter less than 0");
      throw new DataSourceException(ReasonType.INVALID_REQUEST,
          "samplingInterval parameter was larger than time range.");
    }
    else {
      // got a good interval
      intervalMilliseconds = intervalMinutes * minutesToMilliseconds;
    }
    // DEBUG
    // System.out.format("%nstartTime=%s, endTime=%s, interval=%d min%n", startTime, endTime,
    // intervalMilliseconds / minutesToMilliseconds);

    Source source = this.dbManager.getSource(sourceName);
    List<Source> subSources = null;
    if (displaySubsources && source.isVirtual()) {
      subSources = this.dbManager.getAllNonVirtualSubSources(source);
      // System.out.println("subSources: " + subSources); // DEBUG
    }

    // Sets up the columns requested by any SELECT in the datasource query
    List<ColumnDescription> requiredColumns =
        getRequiredColumns(query, CALCULATED_TABLE_COLUMNS, subSources);
    data.addColumns(requiredColumns);

    // Build list of timestamps, starting with startTime, separated by intervalMilliseconds
    List<XMLGregorianCalendar> timestampList =
        Tstamp.getTimestampList(startTime, endTime, intervalMinutes);

    for (int i = 0; i < timestampList.size(); i++, powerData = null, energyData = null, carbonData =
        null) {
      XMLGregorianCalendar currentTimestamp = timestampList.get(i);
      XMLGregorianCalendar previousTimestamp;
      if (i == 0) {
        // First timestamp in list doesn't have a previous timestamp we can use to make an
        // interval, so we look one interval _before_ the first timestamp.
        previousTimestamp = Tstamp.incrementMilliseconds(currentTimestamp, -intervalMilliseconds);
      }
      else {
        previousTimestamp = timestampList.get(i - 1);
      }
      int currentInterval =
          (int) (Tstamp.diff(previousTimestamp, currentTimestamp) / minutesToMilliseconds);

      TableRow row = new TableRow();
      for (ColumnDescription selectionColumn : requiredColumns) {
        String columnName = selectionColumn.getId();
        // If this is a subsource, then the custom property sourceName will be set
        String propertySourceName = selectionColumn.getCustomProperty("sourceName");
        String currentSourceName;
        if (propertySourceName == null) {
          // normal column, not subsource
          currentSourceName = sourceName;
        }
        else {
          // subsource, so use source name from column's custom property
          currentSourceName = propertySourceName;
        }
        try {
          if (columnName.equals(TIME_POINT_COLUMN)) {
            row.addCell(new DateTimeValue(convertTimestamp(currentTimestamp)));
          }
          else if (columnName.endsWith(POWER_CONSUMED_COLUMN)) {
            powerData = this.dbManager.getPower(currentSourceName, currentTimestamp);
            if (powerData == null) {
              row.addCell(0);
            }
            else {
              row.addCell(powerData.getPropertyAsDouble(SensorData.POWER_CONSUMED));
            }
          }
          else if (columnName.endsWith(POWER_GENERATED_COLUMN)) {
            powerData = this.dbManager.getPower(currentSourceName, currentTimestamp);
            if (powerData == null) {
              row.addCell(0);
            }
            else {
              row.addCell(powerData.getPropertyAsDouble(SensorData.POWER_GENERATED));
            }
          }
          else if (columnName.endsWith(ENERGY_CONSUMED_COLUMN)) {
            energyData =
                this.dbManager.getEnergy(currentSourceName, previousTimestamp, currentTimestamp,
                    currentInterval);
            if (energyData == null) {
              row.addCell(0);
            }
            else {
              row.addCell(energyData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED));
            }
          }
          else if (columnName.endsWith(ENERGY_GENERATED_COLUMN)) {
            energyData =
                this.dbManager.getEnergy(currentSourceName, previousTimestamp, currentTimestamp,
                    currentInterval);
            if (energyData == null) {
              row.addCell(0);
            }
            else {
              row.addCell(energyData.getPropertyAsDouble(SensorData.ENERGY_GENERATED));
            }
          }
          else if (columnName.endsWith(CARBON_EMITTED_COLUMN)) {
            carbonData =
                this.dbManager.getCarbon(currentSourceName, previousTimestamp, currentTimestamp,
                    currentInterval);
            if (carbonData == null) {
              row.addCell(0);
            }
            else {
              row.addCell(carbonData.getPropertyAsDouble(SensorData.CARBON_EMITTED));
            }
          }
        }
        catch (NumberFormatException e) {
          // String value in database couldn't be converted to a number.
          throw new DataSourceException(ReasonType.INTERNAL_ERROR, "Found bad number in database"); // NOPMD
        }
      }
      try {
        data.addRow(row);
      }
      catch (TypeMismatchException e) {
        throw new DataSourceException(ReasonType.INTERNAL_ERROR, "Problem adding data to table"); // NOPMD
      }
    }
    return data;
  }

  /**
   * Converts the raw Map returned by HttpServletRequest.getParameterMap() into a generic Map with
   * proper parameters.
   * 
   * @param request The incoming HTTP request.
   * @return A Map<String, String[]>, which is the format getParameterMap() is documented to return.
   */
  @SuppressWarnings(value = "unchecked")
  private Map<String, String[]> getParameterMapGenerics(HttpServletRequest request) {
    // Force the cast, which should work based on the HttpServletRequest Javadoc
    // Map<String, String[]> returnMap = request.getParameterMap();
    // return returnMap;
    return request.getParameterMap();
  }

  /**
   * Retrieves the named HTTP query parameter from the request as a String, or null if the parameter
   * isn't present or has an empty value. ServletRequest returns String arrays, but for HTTP they
   * seem to always contain one element, so this method makes things more convenient.
   * 
   * @param request The incoming HTTP request.
   * @param parameterName The name of the HTTP query parameter
   * @return a String containing the query parameter's value, or null if the parameter name is not
   * present.
   */
  private String getQueryParameter(HttpServletRequest request, String parameterName) {
    Map<String, String[]> parameterMap = getParameterMapGenerics(request);
    String[] value = parameterMap.get(parameterName);
    if ((value == null) || (value.length == 0)) {
      return null;
    }
    else if (value.length == 1) {
      return value[0];
    }
    else {
      log("Got multiple values in parameter array, just returning first value.");
      return value[0];
    }
  }

  /**
   * Takes an XMLGregorianCalendar (the native WattDepot timestamp format) and returns a
   * com.ibm.icu.util.GregorianCalendar (<b>note:</b> this is <b>not</b> the same as a
   * java.util.GregorianCalendar!) with the time zone set to GMT, but with the timestamp adjusted by
   * any time zone offset in the XMLGregorianCalendar.
   * 
   * For example, if the input value is 10 AM Hawaii Standard Time (HST is -10 hours from UTC), then
   * the output will be 10 AM UTC, effectively subtracting 10 hours from the timestamp (in addition
   * to the conversion between types).
   * 
   * This rigamarole is needed because the Google Visualization data source library only accepts
   * timestamps in UTC, and the Annonated Timeline visualization displays the UTC values. Without
   * this conversion, any graphs of meter data recorded in the Hawaii time zone (for instance) would
   * display the data 10 hours later in the graph, which is not acceptable.
   * 
   * @param timestamp the input timestamp, presumably from WattDepot
   * @return a com.ibm.icu.util.GregorianCalendar suitable for use by the Google visualization data
   * source library, and normalized by any time zone offset in timestamp but with timeZone field set
   * to GMT.
   */
  protected com.ibm.icu.util.GregorianCalendar convertTimestamp(XMLGregorianCalendar timestamp) {
    return this.convertTimestamp(timestamp, timestamp.getTimezone());
  }

  /**
   * Takes an XMLGregorianCalendar (the native WattDepot timestamp format) and a time zone offset
   * (expressed as minutes from UTC) and returns a com.ibm.icu.util.GregorianCalendar (<b>note:</b>
   * this is <b>not</b> the same as a java.util.GregorianCalendar!) with the time zone set to GMT,
   * but with the timestamp adjusted by the provided offset.
   * 
   * For example, if the input value is 10 AM Hawaii Standard Time (HST is -10 hours from UTC), and
   * the offset is -600 minutes then the output will be 10 AM UTC, effectively subtracting 10 hours
   * from the timestamp (in addition to the conversion between types).
   * 
   * This rigamarole is needed because the Google Visualization data source library only accepts
   * timestamps in UTC, and the Annonated Timeline visualization displays the UTC values. Without
   * this conversion, any graphs of meter data recorded in the Hawaii time zone (for instance) would
   * display the data 10 hours later in the graph, which is not acceptable. The timezoneOffset is
   * provided if in the future we want the capability to display data normalized to an arbitrary
   * time zone, rather than just the one the data was collected in.
   * 
   * @param timestamp the input timestamp, presumably from WattDepot
   * @param timeZoneOffset the desired offset in minutes from UTC.
   * @return a com.ibm.icu.util.GregorianCalendar suitable for use by the Google visualization data
   * source library, and normalized by timeZoneOffset but with timeZone field set to GMT.
   */
  protected com.ibm.icu.util.GregorianCalendar convertTimestamp(XMLGregorianCalendar timestamp,
      int timeZoneOffset) {
    // Calendar conversion hell. GViz library uses com.ibm.icu.util.GregorianCalendar, which is
    // different from java.util.GregorianCalendar. So we go from our native format
    // XMLGregorianCalendar -> GregorianCalendar, extract milliseconds since epoch, feed that
    // to the IBM GregorianCalendar.
    com.ibm.icu.util.GregorianCalendar gvizCal = new com.ibm.icu.util.GregorianCalendar();
    // Added bonus, DateTimeValue only accepts GregorianCalendars if the timezone is GMT.
    gvizCal.setTimeZone(TimeZone.getTimeZone("GMT"));
    // System.err.println("timestamp: " + timestamp.toString()); // DEBUG
    GregorianCalendar standardCal = timestamp.toGregorianCalendar();
    // System.err.println("standardCal: " + standardCal.toString()); // DEBUG
    // Add the timeZoneOffset to the epoch time after converting to milliseconds
    gvizCal.setTimeInMillis(standardCal.getTimeInMillis()
        + (timeZoneOffset * MILLISECONDS_PER_MINUTE));
    // System.err.println("gvizCal: " + gvizCal.toString()); // DEBUG
    return gvizCal;
  }

  /**
   * @see com.google.visualization.datasource.DataSourceServlet#isRestrictedAccessMode()
   * @return True if the servlet should only respond to queries from the same domain, false
   * otherwise.
   */
  @Override
  protected boolean isRestrictedAccessMode() {
    return false;
  }

  /**
   * Indicates that this data source can select specified columns, rather than fetching or computing
   * data for all possible columns and making the visualization library do the selection.
   * 
   * @return The SELECT capability.
   */
  @Override
  public Capabilities getCapabilities() {
    return Capabilities.SELECT;
  }

  /**
   * Returns true if the given column name is requested in the given query. If the query is empty,
   * all columnNames returns true.
   * 
   * @param query The given query.
   * @param columnName The requested column name.
   * @return True if the given column name is requested in the given query.
   */
  private boolean isColumnRequested(Query query, String columnName) {
    // If the query is empty returns true, as if all columns were specified.
    if (query.isEmpty()) {
      return true;
    }
    // Returns true if the requested column id was specified (not case sensitive).
    List<AbstractColumn> columns = query.getSelection().getColumns();
    for (AbstractColumn column : columns) {
      if (column.getId().equalsIgnoreCase(columnName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a list of required columns based on the query and the actual columns.
   * 
   * @param query The user selection query.
   * @param availableColumns The list of possible columns.
   * @param subsourceList The list of subsources, or null if subsource display is not desired.
   * @return A List of required columns for the requested data table.
   */
  private List<ColumnDescription> getRequiredColumns(Query query,
      ColumnDescription[] availableColumns, List<Source> subsourceList) {
    List<ColumnDescription> requiredColumns = Lists.newArrayList();
    for (ColumnDescription column : availableColumns) {
      if (isColumnRequested(query, column.getId())) {
        // Always add the column without prefix
        requiredColumns.add(column);
        // Only create subsource columns if we have subsources, and don't do it for timePoint
        if ((subsourceList != null) && (!subsourceList.isEmpty())
            && (!column.getId().equals(TIME_POINT_COLUMN))) {
          for (Source subsource : subsourceList) {
            String subsourceName = subsource.getName();
            String subsourceColumnId = subsourceName + column.getId();
            String subsourceLabel = subsourceName + " " + column.getLabel();
            // System.out.println("Column subsource ID: " + subsourceColumnId +
            // ", subsource label: "
            // + subsourceLabel); // DEBUG
            ColumnDescription colDec =
                new ColumnDescription(subsourceColumnId, column.getType(), subsourceLabel);
            colDec.setCustomProperty("sourceName", subsourceName);
            requiredColumns.add(colDec);
          }
        }
      }
    }
    return requiredColumns;
  }
}