package logpp.aggregatorstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

// Count DOI name per month.
// Output as CSV Chunks
public class DOICountCSVAggregatorStrategy implements AggregatorStrategy {
  long inputCount = 0;

  // How to truncate dates.
  private DateProjector dateProjector = new TruncateMonth();

  // If the DOI isn't resolved at least 1 time per month, don't bother.
  static Integer PER_MONTH_CUTOFF = 1;

  // Map of Domain string => Date string => count.
  HashMap<String, Map<String, Integer>> counter;

  Partitioner partitioner;

  public DOICountCSVAggregatorStrategy() {
    this.reset();
  }

  public String toString() {
    return String.format("DomainAggregatorStrategy, %d partitions", this.numPartitions());
  }

  public int numPartitions() {
    return 20;
  }

  public String fileName(String date) {
    return String.format("%s-month-doi.csv-chunks", date);
  }

  public void reset() {
    // In tests, it doesn't make any difference pre-dimensioning to either 1,000,000 or 10,000,000.
    this.counter = new HashMap<String, Map<String, Integer>>();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  public int partition(String[] line) {
    // Use DOI.
    return this.partitioner.partition(line[1]);
  } 

  // line is [date, doi, code, full-domain, subdomains, domain]
  public void feed(String[] line) {
    String doi = line[1];
    String date = line[0];

    String projectedDate = this.dateProjector.project(date);

    Map<String, Integer> dateCounter = this.counter.get(doi);
    if (dateCounter == null) {
      dateCounter = new HashMap<String, Integer>();
      this.counter.put(doi, dateCounter);
    }
    dateCounter.put(projectedDate, dateCounter.getOrDefault(projectedDate, 0) + 1);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. Frequency table size: %d. \n", this.inputCount, this.counter.size());
    }
  }

  public void write(Writer writer) throws IOException {
    for (Map.Entry<String, Map<String, Integer>> doiEntry : this.counter.entrySet()) {
      // If the total for this month for this DOI isn't over the threshold, don't write.
      // Otherwise we get lots of single 'doi was resolved once on this date ever's.
      Integer monthTotal = 0;
      for (Map.Entry<String, Integer> dateEntry : doiEntry.getValue().entrySet()) {
        monthTotal += dateEntry.getValue();
      }

      if (monthTotal < PER_MONTH_CUTOFF) {
        continue;
      }

      writer.write(doiEntry.getKey());
      writer.write("\n");

      for (Map.Entry<String, Integer> dateEntry : doiEntry.getValue().entrySet()) {
        writer.write(dateEntry.getKey());
        writer.write(",");
        writer.write(dateEntry.getValue().toString());
        writer.write("\n");
      }

      writer.write("\n");
    }
  }
}
