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

  // If the DOI isn't resolved at least one on most days, don't bother.
  static Integer PER_MONTH_CUTOFF = 20;

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
