package org.wattdepot.resource.sensordata;

import java.util.List;
import org.wattdepot.resource.source.jaxb.Source;

/**
 * Represents a source and a list of SensorDataStraddles from that source.
 * 
 * @author Robert Brewer
 */
public class StraddleList {
  private Source source;
  
  private List<SensorDataStraddle> straddleList;

  /**
   * Creates the new StraddleList with the given parameters.
   * 
   * @param source The Source.
   * @param straddleList The list of SensorDataStraddles.
   */
  public StraddleList(Source source, List<SensorDataStraddle> straddleList) {
    super();
    this.source = source;
    this.straddleList = straddleList;
  }

  /**
   * @return the source
   */
  public Source getSource() {
    return source;
  }

  /**
   * @param source the source to set
   */
  public void setSource(Source source) {
    this.source = source;
  }

  /**
   * @return the straddleList
   */
  public List<SensorDataStraddle> getStraddleList() {
    return straddleList;
  }

  /**
   * @param straddleList the straddleList to set
   */
  public void setStraddleList(List<SensorDataStraddle> straddleList) {
    this.straddleList = straddleList;
  }
}
