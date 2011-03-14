package org.wattdepot.test.functional;

import static org.junit.Assert.assertEquals;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.util.tstamp.Tstamp;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.OscarRowParser;
import org.wattdepot.datainput.RowParseException;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.server.db.DbException;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.test.ServerTestHelper;
import org.wattdepot.util.UriUtils;

/**
 * Tests WattDepot server loaded with Oscar data for functionality to be used in a programming
 * assignment. Thus it tests functionality desired by a user of the system, so I'm calling it a
 * functional test.
 * 
 * @author Robert Brewer
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class TestOscarFunctionality extends ServerTestHelper {

  /**
   * Different conceptual types of power that WattDepot can handle.
   * 
   * @author Robert Brewer
   */
  public enum Direction {

    /** Represents power generated. */
    GENERATED,
    /** Represents power consumed. */
    CONSUMED
  };

  /**
   * Describes which type of statistic is of interest.
   * 
   * @author Robert Brewer
   */
  public enum StatisticType {

    /** Minimum value desired. */
    MIN,
    /** Maximum value desired. */
    MAX,
    /** Average value desired. */
    AVERAGE
  };

  private WattDepotClient client;

  private static final String lineSep = System.getProperty("line.separator");

  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US);

  /**
   * Creates the new test object, and makes a client for tests to use.
   */
  public TestOscarFunctionality() {
    super();
    this.client = new WattDepotClient(getHostName());
  }

  /**
   * Programmatically loads some Oscar data needed by the rest of the tests.
   * 
   * @throws RowParseException If there is a problem parsing the static data (should never happen).
   * @throws JAXBException If there are problems marshalling the XML dat
   * @throws DbException If there is a problem storing the test data in the database
   * 
   */
  // PMD going wild on the test data
  @BeforeClass
  public static void loadOscarData() throws RowParseException, JAXBException, DbException {
    // Some sim data from Oscar to load into the system before tests
    String[] oscarRows =
        { "2009-10-12T00:00:00-1000,SIM_HPOWER,46,5,BASELOAD",
            "2009-10-12T00:00:00-1000,SIM_KAHE_6,135,8,BASELOAD",
            "2009-10-12T00:00:00-1000,SIM_KAHE_5,88,4,BASELOAD",
            "2009-10-12T00:00:00-1000,SIM_KAHE_4,88,4,BASELOAD",
            "2009-10-12T00:00:00-1000,SIM_KAHE_3,88,4,BASELOAD",
            "2009-10-12T00:00:00-1000,SIM_KAHE_2,55,4,BASELOAD",
            "2009-10-12T00:00:00-1000,SIM_KAHE_1,0,4,BASELOAD",
            "2009-10-12T00:00:00-1000,SIM_KALAELOA,0,5,BASELOAD",
            "2009-10-12T00:00:00-1000,SIM_WAIAU_5,0,0,CYCLING",
            "2009-10-12T00:00:00-1000,SIM_WAIAU_6,0,0,CYCLING",
            "2009-10-12T00:00:00-1000,SIM_HONOLULU_8,0,0,CYCLING",
            "2009-10-12T00:00:00-1000,SIM_HONOLULU_9,0,0,CYCLING",
            "2009-10-12T00:00:00-1000,SIM_WAIAU_9,0,0,PEAKING",
            "2009-10-12T00:00:00-1000,SIM_WAIAU_10,0,0,PEAKING",
            "2009-10-12T00:15:00-1000,SIM_HPOWER,46,5,BASELOAD",
            "2009-10-12T00:15:00-1000,SIM_KAHE_6,135,8,BASELOAD",
            "2009-10-12T00:15:00-1000,SIM_KAHE_5,88,4,BASELOAD",
            "2009-10-12T00:15:00-1000,SIM_KAHE_4,88,4,BASELOAD",
            "2009-10-12T00:15:00-1000,SIM_KAHE_3,88,4,BASELOAD",
            "2009-10-12T00:15:00-1000,SIM_KAHE_2,64,4,BASELOAD",
            "2009-10-12T00:15:00-1000,SIM_KAHE_1,0,4,BASELOAD",
            "2009-10-12T00:15:00-1000,SIM_KALAELOA,0,5,BASELOAD",
            "2009-10-12T00:15:00-1000,SIM_WAIAU_5,0,0,CYCLING",
            "2009-10-12T00:15:00-1000,SIM_WAIAU_6,0,0,CYCLING",
            "2009-10-12T00:15:00-1000,SIM_HONOLULU_8,0,0,CYCLING",
            "2009-10-12T00:15:00-1000,SIM_HONOLULU_9,0,0,CYCLING",
            "2009-10-12T00:15:00-1000,SIM_WAIAU_9,0,0,PEAKING",
            "2009-10-12T00:15:00-1000,SIM_WAIAU_10,0,0,PEAKING",
            "2009-10-12T00:30:00-1000,SIM_HPOWER,46,5,BASELOAD",
            "2009-10-12T00:30:00-1000,SIM_KAHE_6,135,8,BASELOAD",
            "2009-10-12T00:30:00-1000,SIM_KAHE_5,88,4,BASELOAD",
            "2009-10-12T00:30:00-1000,SIM_KAHE_4,88,4,BASELOAD",
            "2009-10-12T00:30:00-1000,SIM_KAHE_3,88,4,BASELOAD",
            "2009-10-12T00:30:00-1000,SIM_KAHE_2,50,4,BASELOAD",
            "2009-10-12T00:30:00-1000,SIM_KAHE_1,0,4,BASELOAD",
            "2009-10-12T00:30:00-1000,SIM_KALAELOA,0,5,BASELOAD",
            "2009-10-12T00:30:00-1000,SIM_WAIAU_5,0,0,CYCLING",
            "2009-10-12T00:30:00-1000,SIM_WAIAU_6,0,0,CYCLING",
            "2009-10-12T00:30:00-1000,SIM_HONOLULU_8,0,0,CYCLING",
            "2009-10-12T00:30:00-1000,SIM_HONOLULU_9,0,0,CYCLING",
            "2009-10-12T00:30:00-1000,SIM_WAIAU_9,0,0,PEAKING",
            "2009-10-12T00:30:00-1000,SIM_WAIAU_10,0,0,PEAKING" };
    String[] sourceXmlStrings =
        {
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_HPOWER</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>HPOWER is an independent power producer on Oahu's grid that uses municipal waste as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>150</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_KAHE_1</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Kahe 1 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>1744</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_KAHE_2</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Kahe 2 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>1744</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_KAHE_3</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Kahe 3 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>1744</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_KAHE_4</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Kahe 4 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>1744</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_KAHE_5</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Kahe 5 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>1744</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_KAHE_6</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Kahe 6 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>1744</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_KALAELOA</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Kalaeloa is an independent power producer on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>2050</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_WAIAU_5</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Waiau 5 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>1800</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_WAIAU_6</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Waiau 6 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>1800</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_HONOLULU_8</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Honolulu 8 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>2240</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_HONOLULU_9</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Honolulu 9 is a HECO plant on Oahu's grid that uses LSFO as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>2240</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_WAIAU_9</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Waiau 9 is a HECO plant on Oahu's grid that uses diesel as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>2400</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_WAIAU_10</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>false</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Waiau 10 is a HECO plant on Oahu's grid that uses diesel as its fuel.</Description>"
                + "<Properties>" + " <Property>" + "  <Key>carbonIntensity</Key>"
                + "  <Value>2400</Value>" + " </Property>" + "</Properties>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_KAHE</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>" + "<Virtual>true</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>" + "<Location>To be looked up later</Location>"
                + "<Description>Virtual resource for all Kahe power plants.</Description>"
                + "<SubSources>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KAHE_1</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KAHE_2</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KAHE_3</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KAHE_4</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KAHE_5</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KAHE_6</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KAHE_7</Href>"
                + "</SubSources>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_WAIAU</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>" + "<Virtual>true</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>" + "<Location>To be looked up later</Location>"
                + "<Description>Virtual resource for all Waiau power plants.</Description>"
                + "<SubSources>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_WAIAU_5</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_WAIAU_6</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_WAIAU_7</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_WAIAU_8</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_WAIAU_9</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_WAIAU_10</Href>"
                + "</SubSources>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_HONOLULU</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>" + "<Virtual>true</Virtual>"
                + "<Coordinates>21.306278,-157.863997,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Virtual resource for all Honolulu power plants.</Description>"
                + "<SubSources>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_HONOLULU_8</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_HONOLULU_9</Href>"
                + "</SubSources>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_IPP</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>"
                + "<Virtual>true</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>"
                + "<Location>To be looked up later</Location>"
                + "<Description>Virtual resource for all independent power producers (non-HECO).</Description>"
                + "<SubSources>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_AES</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_HPOWER</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KALAELOA</Href>"
                + "</SubSources>" + "</Source>",
            "<?xml version=\"1.0\"?>"
                + "<Source>"
                + "<Name>SIM_OAHU_GRID</Name>"
                + "<Owner>http://server.wattdepot.org:1234/wattdepot/users/oscar@wattdepot.org</Owner>"
                + "<Public>true</Public>" + "<Virtual>true</Virtual>"
                + "<Coordinates>0,0,0</Coordinates>" + "<Location>To be looked up later</Location>"
                + "<Description>Virtual resource for all Oahu power plants.</Description>"
                + "<SubSources>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_KAHE</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_WAIAU</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_HONOLULU</Href>"
                + " <Href>http://server.wattdepot.org:1234/wattdepot/sources/SIM_IPP</Href>"
                + "</SubSources>" + "</Source>" };
    // Whacking directly on the database here
    DbManager dbManager =
        new DbManager(server, "org.wattdepot.server.db.memory.MemoryStorageImplementation", true);
    JAXBContext sourceJAXB;
    Unmarshaller unmarshaller;
    sourceJAXB = JAXBContext.newInstance(org.wattdepot.resource.source.jaxb.ObjectFactory.class);
    unmarshaller = sourceJAXB.createUnmarshaller();
    Source source;
    // Go through each Source String in XML format, turn it into a Source, and store in DB
    for (String xmlInput : sourceXmlStrings) {
      source = (Source) unmarshaller.unmarshal(new StringReader(xmlInput));
      // Source read from the file might have an Href elements under SubSources that points to
      // a different host URI. We want all defaults normalized to this server, so update it.
      if (source.isSetSubSources()) {
        List<String> hrefs = source.getSubSources().getHref();
        for (int i = 0; i < hrefs.size(); i++) {
          hrefs.set(i, Source.updateUri(hrefs.get(i), server));
        }
      }
      if (!dbManager.storeSource(source)) {
        throw new DbException("Unable to store source from static XML data");
      }
    }
    OscarRowParser parser = new OscarRowParser("OscarDataConverter", getHostName());
    for (String row : oscarRows) {
      dbManager.storeSensorData(parser.parseRow(row.split(",")));
    }
    server.getContext().getAttributes().put("DbManager", dbManager);
  }

  /**
   * Fetches the Source list from server, checks which sources are parents to which other sources,
   * formats as a pretty string. Solves this assignment problem:
   * http://code.google.com/p/wattdepot/wiki/WattDepotCLI#2.3_list_sources
   * 
   * @throws Exception If there are problems retrieving the Source list.
   */
  @Test
  public void testListSources() throws Exception {
    String expectedOutput =
        "SIM_HONOLULU SIM_OAHU_GRID Virtual resource for all Honolulu power plants."
            + lineSep
            + "SIM_HONOLULU_8 SIM_HONOLULU Honolulu 8 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_HONOLULU_9 SIM_HONOLULU Honolulu 9 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_HPOWER SIM_IPP HPOWER is an independent power producer on Oahu's grid that uses municipal waste as its fuel."
            + lineSep
            + "SIM_IPP SIM_OAHU_GRID Virtual resource for all independent power producers (non-HECO)."
            + lineSep
            + "SIM_KAHE SIM_OAHU_GRID Virtual resource for all Kahe power plants."
            + lineSep
            + "SIM_KAHE_1 SIM_KAHE Kahe 1 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_KAHE_2 SIM_KAHE Kahe 2 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_KAHE_3 SIM_KAHE Kahe 3 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_KAHE_4 SIM_KAHE Kahe 4 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_KAHE_5 SIM_KAHE Kahe 5 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_KAHE_6 SIM_KAHE Kahe 6 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_KALAELOA SIM_IPP Kalaeloa is an independent power producer on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_OAHU_GRID  Virtual resource for all Oahu power plants."
            + lineSep
            + "SIM_WAIAU SIM_OAHU_GRID Virtual resource for all Waiau power plants."
            + lineSep
            + "SIM_WAIAU_10 SIM_WAIAU Waiau 10 is a HECO plant on Oahu's grid that uses diesel as its fuel."
            + lineSep
            + "SIM_WAIAU_5 SIM_WAIAU Waiau 5 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_WAIAU_6 SIM_WAIAU Waiau 6 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep
            + "SIM_WAIAU_9 SIM_WAIAU Waiau 9 is a HECO plant on Oahu's grid that uses diesel as its fuel."
            + lineSep;
    assertEquals("Generated String doesn't match expected", expectedOutput, listSources());
  }

  /**
   * Fetches the Source list from server, checks which sources are parents to which other sources,
   * formats as a pretty string. Solves this assignment problem:
   * http://code.google.com/p/wattdepot/wiki/WattDepotCLI#2.3_list_sources
   * 
   * @return A pretty printed string of the list of sources.
   * @throws Exception If there are problems retrieving the Source list.
   */
  public String listSources() throws Exception {
    List<Source> sourceList = this.client.getSources();
    StringBuffer outputBuff = new StringBuffer(1000);
    StringBuffer parentListBuff;

    for (Source source : sourceList) {
      outputBuff.append(source.getName());
      outputBuff.append(' ');
      parentListBuff = new StringBuffer(100);
      for (Source possibleParent : sourceList) {
        if (isParent(source, possibleParent)) {
          if (parentListBuff.length() != 0) {
            // already have one parent, so add comma
            parentListBuff.append(", ");
          }
          parentListBuff.append(possibleParent.getName());
        }
      }
      if (parentListBuff.length() == 0) {
        outputBuff.append(' ');
      }
      else {
        outputBuff.append(parentListBuff.toString());
        outputBuff.append(' ');
      }
      outputBuff.append(source.getDescription());
      outputBuff.append(lineSep);
    }
    return outputBuff.toString();
  }

  /**
   * Determines whether a source is a parent of another source.
   * 
   * @param source the Source being checked.
   * @param potentialParent the possible parent source.
   * @return true if the potentialParent is a parent of source, otherwise false.
   */
  private boolean isParent(Source source, Source potentialParent) {
    String sourceUri = source.toUri(this.client.getWattDepotUri());
    if (potentialParent.isSetSubSources()) {
      for (String subSource : potentialParent.getSubSources().getHref()) {
        if (sourceUri.equals(subSource)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Tests displaySourceSummary with some different parameters.
   * 
   * @throws Exception If there are problems retrieving the Source list.
   */
  @Test
  public void testDisplaySourceSummary() throws Exception {
    // Parameter that would come from the command line
    String sourceName;
    String expectedOutput;
    sourceName = "SIM_KAHE_1";
    expectedOutput =
        "Subsources: none" + lineSep
            + "Description: Kahe 1 is a HECO plant on Oahu's grid that uses LSFO as its fuel."
            + lineSep + "Owner: oscar@wattdepot.org" + lineSep + "Location: To be looked up later"
            + lineSep + "Coordinates: 0,0,0" + lineSep
            + "Properties: [Property [key=carbonIntensity, value=1744]]" + lineSep
            + "Earliest data: 2009-10-12T00:00:00.000-10:00" + lineSep
            + "Latest data: 2009-10-12T00:30:00.000-10:00" + lineSep + "Total data points: 3"
            + lineSep;
    assertEquals("Didn't get expected output", expectedOutput, displaySourceSummary(sourceName));

    sourceName = "SIM_KAHE";
    expectedOutput =
        "Subsources: [http://localhost:8183/wattdepot/sources/SIM_KAHE_1, http://localhost:8183/wattdepot/sources/SIM_KAHE_2, http://localhost:8183/wattdepot/sources/SIM_KAHE_3, http://localhost:8183/wattdepot/sources/SIM_KAHE_4, http://localhost:8183/wattdepot/sources/SIM_KAHE_5, http://localhost:8183/wattdepot/sources/SIM_KAHE_6, http://localhost:8183/wattdepot/sources/SIM_KAHE_7]"
            + lineSep
            + "Description: Virtual resource for all Kahe power plants."
            + lineSep
            + "Owner: oscar@wattdepot.org"
            + lineSep
            + "Location: To be looked up later"
            + lineSep
            + "Coordinates: 0,0,0"
            + lineSep
            + "Properties: none"
            + lineSep
            + "Earliest data: 2009-10-12T00:00:00.000-10:00"
            + lineSep
            + "Latest data: 2009-10-12T00:30:00.000-10:00"
            + lineSep
            + "Total data points: 18"
            + lineSep;
    assertEquals("Didn't get expected output", expectedOutput, displaySourceSummary(sourceName));

    sourceName = "SIM_OAHU_GRID";
    expectedOutput =
        "Subsources: [http://localhost:8183/wattdepot/sources/SIM_KAHE, http://localhost:8183/wattdepot/sources/SIM_WAIAU, http://localhost:8183/wattdepot/sources/SIM_HONOLULU, http://localhost:8183/wattdepot/sources/SIM_IPP]"
            + lineSep
            + "Description: Virtual resource for all Oahu power plants."
            + lineSep
            + "Owner: oscar@wattdepot.org"
            + lineSep
            + "Location: To be looked up later"
            + lineSep
            + "Coordinates: 0,0,0"
            + lineSep
            + "Properties: none"
            + lineSep
            + "Earliest data: 2009-10-12T00:00:00.000-10:00"
            + lineSep
            + "Latest data: 2009-10-12T00:30:00.000-10:00"
            + lineSep
            + "Total data points: 42"
            + lineSep;
    assertEquals("Didn't get expected output", expectedOutput, displaySourceSummary(sourceName));
  }

  /**
   * Fetches a Source and SourceSummary from server and produces a pretty summary string. Solves
   * this assignment problem:
   * http://code.google.com/p/wattdepot/wiki/WattDepotCLI#2.4_list_summary_{source}
   * 
   * @param sourceName The name of the Source.
   * @return A formatted String summarizing the Source.
   * @throws Exception If there are problems retrieving the Source list.
   */
  public String displaySourceSummary(String sourceName) throws Exception {
    Source source = this.client.getSource(sourceName);
    SourceSummary summary = this.client.getSourceSummary(sourceName);
    StringBuffer buff = new StringBuffer(1000);

    if (source.isSetSubSources()) {
      buff.append(String.format("Subsources: %s%n", source.getSubSources().toString()));
    }
    else {
      buff.append(String.format("Subsources: none%n"));
    }
    buff.append(String.format("Description: %s%n", source.getDescription()));
    buff.append(String.format("Owner: %s%n", UriUtils.getUriSuffix(source.getOwner())));
    buff.append(String.format("Location: %s%n", source.getLocation()));
    buff.append(String.format("Coordinates: %s%n", source.getCoordinates()));
    if (source.isSetProperties()) {
      buff.append(String.format("Properties: %s%n", source.getProperties().toString()));
    }
    else {
      buff.append(String.format("Properties: none%n"));
    }
    if (summary.isSetFirstSensorData()) {
      buff.append(String.format("Earliest data: %s%n", summary.getFirstSensorData().toString()));
    }
    else {
      buff.append(String.format("Earliest data: none%n"));
    }
    if (summary.isSetLastSensorData()) {
      buff.append(String.format("Latest data: %s%n", summary.getLastSensorData().toString()));
    }
    else {
      buff.append(String.format("Latest data: none%n"));
    }
    buff.append(String.format("Total data points: %d%n", summary.getTotalSensorDatas()));
    // System.out.println(buff.toString());
    return buff.toString();
  }

  /**
   * Tests the displayOneSensorData method.
   * 
   * @throws Exception If there is a problem with anything.
   */
  @Test
  public void testDisplayOneSensorData() throws Exception {
    // These are the parameters that would be coming from the command line
    String sourceName = "SIM_KAHE_2";
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp("2009-10-12T00:15:00.000-10:00");
    String expectedOutput =
        String
            .format("Tool: OscarDataConverter%nSource: SIM_KAHE_2%nProperties: (powerGenerated : 6.4E7)%n");
    assertEquals("Generated string doesn't match expected", expectedOutput, displayOneSensorData(
        sourceName, timestamp));
  }

  /**
   * Fetches one SensorData from WattDepot server, and formats it as a nice string. Solves this
   * assignment problem: http://code.google.com/p/wattdepot/wiki
   * /WattDepotCLI#2.5_list_sensordata_{source}_timestamp_{timestamp}
   * 
   * @param sourceName The name of the source.
   * @param timestamp The timestamp in question.
   * @return A string representing the sensor data.
   * @throws Exception If there is a problem with anything.
   */
  public String displayOneSensorData(String sourceName, XMLGregorianCalendar timestamp)
      throws Exception {
    // These are the parameters that would be coming from the command line
    SensorData data = null;
    data = this.client.getSensorData(sourceName, timestamp);
    StringBuffer buff = new StringBuffer(200);
    buff.append(String.format("Tool: %s%n", data.getTool()));
    buff.append(String.format("Source: %s%n", UriUtils.getUriSuffix(data.getSource())));
    buff.append("Properties: ");
    List<Property> props = data.getProperties().getProperty();
    Property prop;
    for (int i = 0; i < props.size(); i++) {
      prop = props.get(i);
      if (i != 0) {
        // Before each property but the first, prefix with comma
        buff.append(", ");
      }
      buff.append(String.format("(%s : %s)", prop.getKey(), prop.getValue()));
    }
    buff.append(String.format("%n"));
    return buff.toString();
  }

  /**
   * Tests listOneDaySensorData.
   * 
   * @throws Exception If there is a problem with anything.
   */
  @Test
  public void testListOneDaySensorData() throws Exception {
    // These are the parameters that would be coming from the command line
    String sourceName = "SIM_KAHE_2", day = "2009-10-12";
    String expectedOutput =
        "2009-10-12T00:00:00.000-10:00 Tool: OscarDataConverter Properties: [Property [key=powerGenerated, value=5.5E7]]"
            + lineSep
            + "2009-10-12T00:15:00.000-10:00 Tool: OscarDataConverter Properties: [Property [key=powerGenerated, value=6.4E7]]"
            + lineSep
            + "2009-10-12T00:30:00.000-10:00 Tool: OscarDataConverter Properties: [Property [key=powerGenerated, value=5.0E7]]"
            + lineSep;
    Date startDate = this.dateFormat.parse(day);
    assertEquals("Generated string doesn't match expected", expectedOutput, listOneDaySensorData(
        sourceName, startDate));
  }

  /**
   * Fetches one day of SensorData from WattDepot server, and formats it as series of nice lines.
   * Solves this assignment problem:
   * http://code.google.com/p/wattdepot/wiki/WattDepotCLI#2.6_list_sensordata_{source}_day_{day}
   * 
   * @param sourceName The name of the source.
   * @param day A Date object representing the desired day of data.
   * @return A string representing the sensor data.
   * @throws Exception If there is a problem with anything.
   */
  public String listOneDaySensorData(String sourceName, Date day) throws Exception {
    // These are the parameters that would be coming from the command line
    XMLGregorianCalendar startTime, endTime;
    startTime = Tstamp.makeTimestamp(day.getTime());
    endTime = Tstamp.incrementDays(startTime, 1);
    List<SensorData> dataList = this.client.getSensorDatas(sourceName, startTime, endTime);
    StringBuffer buff = new StringBuffer(1000);
    for (SensorData data : dataList) {
      buff.append(data.getTimestamp() + " Tool: " + data.getTool() + " Properties: ");
      buff.append(data.getProperties().toString());
      buff.append(String.format("%n"));
    }
    return buff.toString();
  }

  /**
   * Returns a string with the amount of power consumed or generated by the given source at the
   * given timestamp. Solves assignment problem
   * http://code.google.com/p/wattdepot/wiki/WattDepotCLI#
   * 2.7_list_power_[generated|consumed]_{source}_timestamp_{timestam
   * 
   * @param dir Type of power desired.
   * @param sourceName Name of the source.
   * @param timestamp Timestamp of interest.
   * @return Power requested.
   * @throws WattDepotClientException If there are problems with the client.
   */
  public String listPowerForTimestamp(Direction dir, String sourceName,
      XMLGregorianCalendar timestamp) throws WattDepotClientException {
    switch (dir) {
    case GENERATED:
      return Double.toString(client.getPowerGenerated(sourceName, timestamp));
    case CONSUMED:
      return Double.toString(client.getPowerConsumed(sourceName, timestamp));
    default:
      throw new AssertionError("Unknown dir of power encountered");
    }
  }

  /**
   * Tests listPowerForTimestamp.
   * 
   * @throws WattDepotClientException If there are problems with the client.
   * @throws Exception If there are problems creating the timestamp.
   */
  @Test
  public void listPowerForTimestampTest() throws WattDepotClientException, Exception {
    // Test with non-virtual source
    assertEquals("Did not get expected power from SIM_KAHE_2", "6.28E7", listPowerForTimestamp(
        Direction.GENERATED, "SIM_KAHE_2", Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00")));
    // Test with virtual source
    assertEquals("Did not get expected power from SIM_KAHE", "4.618E8", listPowerForTimestamp(
        Direction.GENERATED, "SIM_KAHE", Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00")));
    // Test with virtual source that includes other sources
    assertEquals("Did not get expected power from SIM_OAHU_GRID", "5.078E8",
        listPowerForTimestamp(Direction.GENERATED, "SIM_OAHU_GRID", Tstamp
            .makeTimestamp("2009-10-12T00:13:00.000-10:00")));
    // Test that power consumed is 0 for timestamp
    assertEquals("Did not get expected power from SIM_OAHU_GRID", "0.0", listPowerForTimestamp(
        Direction.CONSUMED, "SIM_OAHU_GRID", Tstamp.makeTimestamp("2009-10-12T00:13:00.000-10:00")));
  }

  /**
   * Returns a string with the amount of power consumed or generated by the given source for one
   * day. Solves assignment problem
   * http://code.google.com/p/wattdepot-cli/wiki/CommandSpecification#
   * 2.8_list_power_[generated|consumed]_{source}_day_{day}_sampling-
   * 
   * @param dir Type of power desired.
   * @param sourceName Name of the source.
   * @param day Day of interest.
   * @param samplingInterval interval at which to sample, in minutes
   * @param stat The type of statistic requested.
   * @return Power requested.
   * @throws WattDepotClientException If there are problems with the client.
   */
  public String listPowerForDay(Direction dir, String sourceName, Date day, int samplingInterval,
      StatisticType stat) throws WattDepotClientException {
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(day.getTime());
    int minutesInDay = 60 * 24;
    double returnValue, power;

    // Initialize returnValue depending on the type of statistic requested.
    switch (stat) {
    case MIN:
      returnValue = Double.MAX_VALUE;
      break;
    case MAX:
      returnValue = Double.MIN_VALUE;
      break;
    case AVERAGE:
      returnValue = 0;
      break;
    default:
      throw new AssertionError("Unknown type of statistic encountered");
    }

    for (int i = 0; i < minutesInDay; i += samplingInterval, timestamp =
        Tstamp.incrementMinutes(timestamp, samplingInterval)) {
      switch (dir) {
      case GENERATED:
        power = client.getPowerGenerated(sourceName, timestamp);
        break;
      case CONSUMED:
        power = client.getPowerConsumed(sourceName, timestamp);
        break;
      default:
        throw new AssertionError("Unknown type of power encountered");
      }
      switch (stat) {
      case MIN:
        if (power < returnValue) {
          returnValue = power;
        }
        break;
      case MAX:
        if (power > returnValue) {
          returnValue = power;
        }
        break;
      case AVERAGE:
        returnValue += power;
        break;
      default:
        throw new AssertionError("Unknown type of statistic encountered");
      }
    }
    // Divide average total number of samples
    if (stat == StatisticType.AVERAGE) {
      // use integer division to get number of samples (truncation actually desired here)
      int numSamples = minutesInDay / samplingInterval;
      return Double.toString(returnValue / numSamples);
    }
    else {
      return Double.toString(returnValue);
    }
  }

  /**
   * Tests listPowerForDay.
   * 
   * @throws WattDepotClientException If there are problems with the client.
   * @throws Exception If there are problems creating the timestamp.
   */
  @Test
  @Ignore("Needs full day of data to work properly")
  public void listPowerForDayTest() throws WattDepotClientException, Exception {
    // Test with non-virtual source
    assertEquals("Did not get expected power from SIM_KAHE_2", "6.28E7", listPowerForDay(
        Direction.GENERATED, "SIM_KAHE_2", this.dateFormat.parse("2009-10-12"), 1,
        StatisticType.MIN));
    // Test with virtual source
    assertEquals("Did not get expected power from SIM_KAHE", "6.28E7",
        listPowerForDay(Direction.GENERATED, "SIM_KAHE", this.dateFormat.parse("2009-10-12"), 30,
            StatisticType.MIN));
    // Test with virtual source that includes other sources
    assertEquals("Did not get expected power from SIM_OAHU_GRID", "6.28E7", listPowerForDay(
        Direction.GENERATED, "SIM_OAHU_GRID", this.dateFormat.parse("2009-10-12"), 1,
        StatisticType.MIN));
    // Test that power consumed is 0
    assertEquals("Did not get expected power from SIM_OAHU_GRID", "6.28E7", listPowerForDay(
        Direction.CONSUMED, "SIM_OAHU_GRID", this.dateFormat.parse("2009-10-12"), 1,
        StatisticType.MIN));
  }

  /**
   * Returns a string with the amount of energy produced or consumed by the given source for one
   * day. Solves assignment problem
   * http://code.google.com/p/wattdepot-cli/wiki/CommandSpecification#
   * 2.9_list_total_[carbon|energy]_[generated|consumed]_{source}_day
   * 
   * @param dir Type of power desired.
   * @param sourceName Name of the source.
   * @param day Day of interest.
   * @param samplingInterval interval at which to sample, in minutes
   * @return Energy requested.
   * @throws WattDepotClientException If there are problems with the client.
   */
  public String listEnergyForDay(Direction dir, String sourceName, Date day, int samplingInterval)
      throws WattDepotClientException {
    XMLGregorianCalendar startTime, endTime;
    startTime = Tstamp.makeTimestamp(day.getTime());
    endTime = Tstamp.incrementDays(startTime, 1);
    double output;

    if (dir == Direction.CONSUMED) {
      output = client.getEnergyConsumed(sourceName, startTime, endTime, samplingInterval);
    }
    else if (dir == Direction.GENERATED) {
      output = client.getEnergyGenerated(sourceName, startTime, endTime, samplingInterval);
    }
    else {
      // Unknown direction, so abort
      return null;
    }
    return Double.toString(output);
  }

  /**
   * Tests listEnergyForDay.
   * 
   * @throws Exception If there are problems creating the timestamp or the client.
   */
  @Test
  @Ignore("Needs full day of data to work properly")
  public void listEnergyForDayTest() throws Exception {
    // Test with non-virtual source
    assertEquals("Did not get expected energy from SIM_KAHE_2", "6.28E7", listEnergyForDay(
        Direction.GENERATED, "SIM_KAHE_2", this.dateFormat.parse("2009-10-12"), 15));
    // Test with virtual source
    assertEquals("Did not get expected energy from SIM_KAHE", "6.28E7", listEnergyForDay(
        Direction.GENERATED, "SIM_KAHE", this.dateFormat.parse("2009-10-12"), 1));
    // Test with virtual source that includes other sources
    assertEquals("Did not get expected energy from SIM_OAHU_GRID", "6.28E7", listEnergyForDay(
        Direction.GENERATED, "SIM_OAHU_GRID", this.dateFormat.parse("2009-10-12"), 1));
    // Test that power consumed is 0
    assertEquals("Did not get expected energy from SIM_OAHU_GRID", "0.0", listEnergyForDay(
        Direction.CONSUMED, "SIM_KAHE_2", this.dateFormat.parse("2009-10-12"), 1));
  }

  /**
   * Returns a string with the amount of carbon emitted by the given source for one day. Solves
   * assignment problem http://code.google.com/p/wattdepot-cli/wiki/CommandSpecification#
   * 2.9_list_total_[carbon|energy]_[generated|consumed]_{source}_day
   * 
   * @param sourceName Name of the source.
   * @param day Day of interest.
   * @param samplingInterval interval at which to sample, in minutes
   * @return Carbon requested.
   * @throws WattDepotClientException If there are problems with the client.
   */
  public String listCarbonForDay(String sourceName, Date day, int samplingInterval)
      throws WattDepotClientException {
    XMLGregorianCalendar startTime, endTime;
    startTime = Tstamp.makeTimestamp(day.getTime());
    endTime = Tstamp.incrementDays(startTime, 1);
    double output;

    output = client.getCarbonEmitted(sourceName, startTime, endTime, samplingInterval);
    return Double.toString(output);
  }

  /**
   * Tests listCarbonForDay.
   * 
   * @throws Exception If there are problems creating the timestamp or the client.
   */
  @Test
  @Ignore("Needs full day of data to work properly")
  public void listCarbonForDayTest() throws Exception {
    // Test with non-virtual source
    assertEquals("Did not get expected energy from SIM_KAHE_2", "6.28E7", listCarbonForDay(
        "SIM_KAHE_2", this.dateFormat.parse("2009-10-12"), 15));
    // Test with virtual source
    assertEquals("Did not get expected power from SIM_KAHE", "6.28E7", listCarbonForDay("SIM_KAHE",
        this.dateFormat.parse("2009-10-12"), 1));
    // Test with virtual source that includes other sources
    assertEquals("Did not get expected power from SIM_OAHU_GRID", "6.28E7", listCarbonForDay(
        "SIM_OAHU_GRID", this.dateFormat.parse("2009-10-12"), 1));
    // Test that power consumed is 0
    assertEquals("Did not get expected power from SIM_OAHU_GRID", "6.28E7", listCarbonForDay(
        "SIM_OAHU_GRID", this.dateFormat.parse("2009-10-12"), 1));
  }

  /**
   * Returns a URL that displays the power for the given range of days, sampled at the given
   * interval, in the given direction.
   * 
   * @param dir Type of power desired.
   * @param sourceName Name of the source.
   * @param startDay The starting day.
   * @param endDay The starting day.
   * @param samplingInterval interval at which to sample, in minutes
   * @return A String containing the URI for Google Chart API
   * @throws WattDepotClientException If there are problems with the client.
   */
  public String chartPowerForDay(Direction dir, String sourceName, Date startDay, Date endDay,
      int samplingInterval) throws WattDepotClientException {
    double power = 0, minPower = Double.MAX_VALUE, maxPower = Double.MIN_VALUE;
    XMLGregorianCalendar startTime = null, endTime = null, timestamp;
    int maxUriLength = 2048;
    StringBuffer chartUri = new StringBuffer(maxUriLength);

    startTime = Tstamp.makeTimestamp(startDay.getTime());
    endTime = Tstamp.makeTimestamp(endDay.getTime());

    chartUri.append("http://chart.apis.google.com/chart?chs=250x100&cht=lc&chd=t:");
    timestamp = startTime;
    while (Tstamp.lessThan(timestamp, endTime)) {
      // Convert to megawatts
      power = client.getPowerGenerated("SIM_OAHU_GRID", timestamp) / 1000000;
      chartUri.append(String.format("%.1f,", power));
      if (power < minPower) {
        minPower = power;
      }
      if (power > maxPower) {
        maxPower = power;
      }
      timestamp = Tstamp.incrementMinutes(timestamp, samplingInterval);
    }
    // Delete trailing ','
    chartUri.deleteCharAt(chartUri.length() - 1);
    chartUri.append(String.format("&chds=%.1f,%.1f&chxt=y&chxr=0,%.1f,%.1f", minPower, maxPower,
        minPower, maxPower));
    return String.format("Google Charts URI:%n%s%n", chartUri);
  }

  /**
   * Tests listEnergyForDay.
   * 
   * @throws Exception If there are problems creating the timestamp or the client.
   */
  @Test
  @Ignore("Needs full day of data to work properly")
  public void chartPowerForDayTest() throws Exception {
    Date startDay = this.dateFormat.parse("2009-10-30");
    Date endDay = this.dateFormat.parse("2009-11-04");
    // Test with non-virtual source
    assertEquals("Did not get expected energy from SIM_KAHE_2", "6.28E7", chartPowerForDay(
        Direction.GENERATED, "SIM_KAHE_2", startDay, endDay, 15));
    // Test with virtual source
    assertEquals("Did not get expected power from SIM_KAHE", "6.28E7", chartPowerForDay(
        Direction.GENERATED, "SIM_KAHE", startDay, endDay, 1));
    // Test with virtual source that includes other sources
    assertEquals("Did not get expected power from SIM_OAHU_GRID", "6.28E7", chartPowerForDay(
        Direction.GENERATED, "SIM_OAHU_GRID", startDay, endDay, 1));
    // Test that power consumed is 0
    assertEquals("Did not get expected power from SIM_OAHU_GRID", "6.28E7", chartPowerForDay(
        Direction.CONSUMED, "SIM_OAHU_GRID", startDay, endDay, 1));
  }
}
