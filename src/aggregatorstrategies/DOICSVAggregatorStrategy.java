package logpp.aggregatorstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

// Count DOI name per day.
// Output as:
// doi
// «date» «count»
// «date» «count»
// «date» «count»
// ...
// «blank line»
// «repeat»
public class DOICSVAggregatorStrategy implements AggregatorStrategy {
  long inputCount = 0;

  // Map of Domain string => Date string => count.
  HashMap<String, Map<String, Integer>> counter;

  Partitioner partitioner;

  public DOICSVAggregatorStrategy() {
    this.reset();
  }

  public String toString() {
    return String.format("DomainAggregatorStrategy, %d partitions", this.numPartitions());
  }

  public int numPartitions() {
    return 20;
  }

  public String fileName(String date) {
    return String.format("%s-doi.csv-chunks",  date);
  }

  public void reset() {
    this.counter = new HashMap<String, Map<String, Integer>>();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  public int partition(String[] line) {
    // Use DOI.
    return this.partitioner.partition(line[1]);
  } 

  // line is [date, doi, code, domain]
  public void feed(String[] line) {
    String doi = line[1].toLowerCase();
    String date = line[0];

    Map<String, Integer> dateCounter = this.counter.get(doi);
    if (dateCounter == null) {
      dateCounter = new HashMap<String, Integer>();
      this.counter.put(doi, dateCounter);
    }
    dateCounter.put(date, dateCounter.getOrDefault(date, 0) + 1);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. Frequency table size: %d. \n", this.inputCount, this.counter.size());
    }
  }

  public void write(Writer writer) throws IOException {
    // TODO month count threshold? 

    for (Map.Entry<String, Map<String, Integer>> doiEntry : this.counter.entrySet()) {
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
