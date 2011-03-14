package org.wattdepot.tinker;

import java.util.ArrayList;
import javax.servlet.http.HttpServletRequest;
import com.google.visualization.datasource.DataSourceServlet;
import com.google.visualization.datasource.base.TypeMismatchException;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.value.ValueType;
import com.google.visualization.datasource.query.Query;
import com.ibm.icu.util.GregorianCalendar;
import com.ibm.icu.util.TimeZone;

public class QueryTestServlet extends DataSourceServlet {

  /** Keep Eclipse happy. */
  private static final long serialVersionUID = 1L;

  public DataTable generateDataTable(Query query, HttpServletRequest request) {
    // Create a data table,
    DataTable data = new DataTable();
    ArrayList<ColumnDescription> cd = new ArrayList<ColumnDescription>();
    cd.add(new ColumnDescription("datetime", ValueType.DATETIME, "Timestamp"));
    cd.add(new ColumnDescription("pens", ValueType.NUMBER, "Number of Pens Sold"));
    cd.add(new ColumnDescription("isFountain", ValueType.BOOLEAN, "Fountain pen?"));

    data.addColumns(cd);

    // Fill the data table.
    try {
      data.addRowFromValues(makeCal(2009, 9, 29, 20, 0, 0), 5, true);
      data.addRowFromValues(makeCal(2009, 9, 29, 20, 0, 30), 1, false);
      data.addRowFromValues(makeCal(2009, 9, 29, 20, 1, 0), 13, true);
      data.addRowFromValues(makeCal(2009, 9, 29, 20, 1, 49), 2, false);
    }
    catch (TypeMismatchException e) {
      System.out.println("Invalid type!");
    }
    return data;
  }
  
  private GregorianCalendar makeCal(int year, int month, int date, int hour, int minute, int second) {
    GregorianCalendar cal = new GregorianCalendar(year, month, date, hour, minute, second);
    cal.setTimeZone(TimeZone.getTimeZone("GMT"));
    return cal;
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
}
