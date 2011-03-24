//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vhudson-jaxb-ri-2.1-833 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2009.08.31 at 03:06:32 PM HST 
//

package org.wattdepot.resource.source.jaxb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * <p>
 * Java class for anonymous complex type.
 * 
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{}SourceRef" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = { "sourceRef" })
@XmlRootElement(name = "SourceIndex")
public class SourceIndex implements Serializable {

  private final static long serialVersionUID = 12343L;
  @XmlElement(name = "SourceRef")
  protected List<SourceRef> sourceRef;

  /**
   * Default no-argument constructor, apparently needed by JAXB. Don't use this, use the one with
   * all the parameters.
   */
  public SourceIndex() {
    // Apparently needed by JAXB
  }

  /**
   * Creates a SourceIndex with the requested capacity.
   */
  public SourceIndex(int capacity) {
    this.sourceRef = new ArrayList<SourceRef>(capacity);
  }

  /**
   * Gets the value of the sourceRef property.
   * 
   * <p>
   * This accessor method returns a reference to the live list, not a snapshot. Therefore any
   * modification you make to the returned list will be present inside the JAXB object. This is why
   * there is not a <CODE>set</CODE> method for the sourceRef property.
   * 
   * <p>
   * For example, to add a new item, do as follows:
   * 
   * <pre>
   * getSourceRef().add(newItem);
   * </pre>
   * 
   * 
   * <p>
   * Objects of the following type(s) are allowed in the list {@link SourceRef }
   * 
   * 
   */
  public List<SourceRef> getSourceRef() {
    if (sourceRef == null) {
      sourceRef = new ArrayList<SourceRef>();
    }
    return this.sourceRef;
  }

  public boolean isSetSourceRef() {
    return ((this.sourceRef != null) && (!this.sourceRef.isEmpty()));
  }

  public void unsetSourceRef() {
    this.sourceRef = null;
  }

  // Broke down and added these manually to the generated code. It would be better if they were
  // automatically generated via XJC plugins, but that required a bunch of dependencies that I
  // was unwilling to deal with right now. If the schema files change, this code will be blown
  // away, so there are unit tests that confirm that equals and hashCode work to guard against
  // that.

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((sourceRef == null) ? 0 : sourceRef.hashCode());
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SourceIndex other = (SourceIndex) obj;
    if (sourceRef == null) {
      if (other.sourceRef != null) {
        return false;
      }
    }
    else if (!sourceRef.equals(other.sourceRef)) {
      return false;
    }
    return true;
  }

}