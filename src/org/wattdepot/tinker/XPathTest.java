package org.wattdepot.tinker;

import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Class used to try out XPath in Java.
 * 
 * @author Robert Brewer
 */
public class XPathTest {

  /**
   * Parses the filename given in first arg assuming it is TED 5000 XML and tries to extract useful
   * values.
   * 
   * @param args String array of command line arguments
   * @throws ParserConfigurationException If there are problems configuring.
   * @throws IOException If there are problems reading the file.
   * @throws SAXException If there are problems parsing.
   * @throws XPathExpressionException If there is a problem with the XPath expression.
   */
  public static void main(String[] args) throws ParserConfigurationException, SAXException,
      IOException, XPathExpressionException {
    if (args.length != 1) {
      System.out.println("Need XML filename arg.");
      return;
    }
    DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
    domFactory.setNamespaceAware(true); // never forget this!
    DocumentBuilder builder = domFactory.newDocumentBuilder();
    Document doc = builder.parse(args[0]);

    XPathFactory factory = XPathFactory.newInstance();
    XPath powerXpath = factory.newXPath();
    XPath energyXpath = factory.newXPath();
    XPathExpression exprPower = powerXpath.compile("/LiveData/Power/Total/PowerNow/text()");
    XPathExpression exprEnergy = energyXpath.compile("/LiveData/Power/Total/PowerMTD/text()");
    Object powerResult = exprPower.evaluate(doc, XPathConstants.NUMBER);
    Object energyResult = exprEnergy.evaluate(doc, XPathConstants.NUMBER);

    Double power = (Double) powerResult;
    Double energy = (Double) energyResult;
    System.out.println("Power from TED 5K: " + power + "W");
    System.out.println("Energy from TED 5K month to date: " + energy + "Wh");
  }
}
