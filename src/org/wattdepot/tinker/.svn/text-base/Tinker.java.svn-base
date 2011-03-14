package org.wattdepot.tinker;

import java.util.GregorianCalendar;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

public class Tinker {

  /**
   * @param args
   * @throws DatatypeConfigurationException 
   */
  public static void main(String[] args) throws DatatypeConfigurationException {
//    XMLGregorianCalendar xmlCal = DatatypeFactory.newInstance().newXMLGregorianCalendar();
    
    DatatypeFactory factory = DatatypeFactory.newInstance();
    XMLGregorianCalendar xmlCal = factory.newXMLGregorianCalendar(new GregorianCalendar());
    
//    Calendar now = Calendar.getInstance();
//    xmlCal.setMonth(now.get(Calendar.MONTH)+1);
//    xmlCal.setYear(now.get(Calendar.YEAR));
//    xmlCal.setTime(now.get(Calendar.HOUR_OF_DAY), 
//    now.get(Calendar.MINUTE), 
//    now.get(Calendar.SECOND));
    System.out.println(xmlCal.toString());
  }
}
