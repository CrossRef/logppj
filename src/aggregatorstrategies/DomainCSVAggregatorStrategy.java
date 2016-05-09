package logpp.aggregatorstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

// Count referring domain name per day.
// Output as:
// «domain»
// «date» «count»
// «date» «count»
// «date» «count»
// ...
// «blank line»
// «repeat»
public class DomainCSVAggregatorStrategy implements AggregatorStrategy {
  // Month or day?
  boolean monthMode = false;

  long inputCount = 0;

  // Map of Domain string => Date string => count.
  HashMap<String, Map<String, Integer>> counter;

  Partitioner partitioner;

  public DomainCSVAggregatorStrategy(boolean monthMode) {
    this.reset();
    this.monthMode = monthMode;
  }

  public String toString() {
    return String.format("DomainCSVAggregatorStrategy, %d partitions", this.numPartitions());
  }

  public int numPartitions() {
    if (this.monthMode) {
      return 2;
    } else {
      return 10;
    }
  }

  public String fileName(String date) {
    if (this.monthMode) {
      return String.format("%s-month-domain.csv-chunks",  date);
    } else {
      return String.format("%s-day-domain.csv-chunks",  date);
    }
  }

  public void reset() {
    this.counter = new HashMap<String, Map<String, Integer>>();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  public int partition(String[] line) {
    // Use domain.
    return this.partitioner.partition(line[3]);
  } 

  // line is [date, doi, code, domain]
  public void feed(String[] line) {
    String domain = line[3];
    String date = line[0];

    if (this.monthMode) {
      // Truncate to first day of the month.
      date = date.substring(0, 7) + "-01";
    }

    Map<String, Integer> dateCounter = this.counter.get(domain);
    if (dateCounter == null) {
      dateCounter = new HashMap<String, Integer>();
      this.counter.put(domain, dateCounter);
    }
    dateCounter.put(date, dateCounter.getOrDefault(date, 0) + 1);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. Frequency table size: %d. \n", this.inputCount, this.counter.size());
    }
  }

  public void write(Writer writer) throws IOException {
    for (Map.Entry<String, Map<String, Integer>> domainEntry : this.counter.entrySet()) {
      writer.write(domainEntry.getKey());
      writer.write("\n");

      for (Map.Entry<String, Integer> dateEntry : domainEntry.getValue().entrySet()) {
        writer.write(dateEntry.getKey());
        writer.write(",");
        writer.write(dateEntry.getValue().toString());
        writer.write("\n");
      }

      writer.write("\n");
    }
  }
}
