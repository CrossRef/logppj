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
  // One of MODE_MONTH, MODE_DAY
  // Can't be MODE_YEAR because the files come to us per month so resut would be the same.
  int mode = Constants.MODE_DAY;

  long inputCount = 0;

  // Map of Domain string => Date string => count.
  Counter2d counter = new Counter2d();

  Partitioner partitioner;

  public DomainCSVAggregatorStrategy(int mode) {
    this.reset();
    this.mode = mode;
  }

  public String toString() {
    return String.format("DomainCSVAggregatorStrategy, %d partitions", this.numPartitions());
  }

  public int numPartitions() {
    switch (this.mode) {
      case Constants.MODE_MONTH: return 2; 
      case Constants.MODE_DAY: return 5; 
      default: return 10; 
    }
  }

  public String fileName(String date) {
    switch (this.mode) {
      case Constants.MODE_MONTH: return String.format("%s-month-domain.csv-chunks",  date); 
      case Constants.MODE_DAY: return String.format("%s-day-domain.csv-chunks",  date); 
      default: return String.format("%s-domain.csv-chunks",  date); 
    }
  }

  public void reset() {
    this.counter = new Counter2d();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  // [date, doi, code, full-domain, subdomains, domain]
  public int partition(String[] line) {
    return this.partitioner.partition(line[5]);
  } 

  // line is [date, doi, code, full-domain, subdomain, domain]
  public void feed(String[] line) {
    String domain = line[5];
    String date = line[0];

    // Truncate if necessary.
    switch (this.mode) {
      case Constants.MODE_MONTH: date = date.substring(0, 7) + "-01"; break;
    }

    this.counter.inc(domain, date);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. \n", this.inputCount);
    }
  }

  public void write(Writer writer) throws IOException {
    this.counter.writeChunks(writer);
  }
}