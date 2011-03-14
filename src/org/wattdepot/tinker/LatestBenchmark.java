package org.wattdepot.tinker;

import java.util.Date;
import java.util.List;
import org.wattdepot.client.ResourceNotFoundException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;

/**
 * Benchmarks how long it takes to retrieve the latest sensor data from each source.
 * 
 * @author Robert Brewer
 */
public class LatestBenchmark {

  /**
   * Runs the benchmark.
   * 
   * @param args
   * @throws Exception If there are problems.
   */
  public static void main(String[] args) throws Exception {
    WattDepotClient client = new WattDepotClient("http://server.wattdepot.org:8182/wattdepot/");
    Date start, end;

    start = new Date();
    List<Source> sources = client.getSources();
    end = new Date();
    System.out.format("Time to retrieve list of sources: %d ms%n%n", end.getTime()
        - start.getTime());

    int nameWidth = 0;
    for (Source source : sources) {
      nameWidth = (nameWidth < source.getName().length()) ? source.getName().length() : nameWidth;
    }

    for (Source source : sources) {
      String sourceName = source.getName();
      start = new Date();
      SourceSummary summary = client.getSourceSummary(sourceName);
      long summaryTime = new Date().getTime() - start.getTime();
      start = new Date();
      try {
        client.getLatestSensorData(sourceName);
        long latestDataTime = new Date().getTime() - start.getTime();
        String formatString =
            "%" + nameWidth
                + "s: summary = %6d ms, latest = %6d ms, # data = %7d, %f ms per sensor data%n";
        System.out.format(formatString, sourceName, summaryTime, latestDataTime, summary
            .getTotalSensorDatas(), (latestDataTime * 1.0) / summary.getTotalSensorDatas());
      }
      catch (ResourceNotFoundException e) {
        // Source has no sensor data
        System.out.format("%s: summary = %d ms, no sensor data%n", sourceName, summaryTime);
      }
    }
  }
}
