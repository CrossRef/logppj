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
public class FullDomainCSVAggregatorStrategy implements AggregatorStrategy {
  // Used to project the date for aggregation, e.g. truncate to the month.
  DateProjector dateProjector;

  long inputCount = 0;

  // Map of Domain string => Date string => count.
  Counter2d counter = new Counter2d();

  Partitioner partitioner;

  public FullDomainCSVAggregatorStrategy(DateProjector dateProjector) {
    this.dateProjector = dateProjector;

    this.reset();
  }

  public String toString() {
    return String.format("FullDomainCSVAggregatorStrategy, %d partitions", this.numPartitions());
  }

  public int numPartitions() {
    switch (this.dateProjector.getName()) {
      case "month": return 2; 
      case "day": return 5; 
      default: return 10; 
    }
  }

  public String fileName(String date) {
    return String.format("%s-%s-fulldomain.csv-chunks", date, this.dateProjector.getName());
  }

  public void reset() {
    this.counter = new Counter2d();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  // line is [date, doi, code, full-domain, subdomain, domain]
  public int partition(String[] line) {
    // Use domain so full-domains with same domain are close to each other.
    return this.partitioner.partition(line[5]);
  }

  // line is [date, doi, code, full-domain, subdomain, domain]
  public void feed(String[] line) {
    String domain = line[3];
    String date = line[0];

    this.counter.inc(domain, this.dateProjector.project(date));

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. \n", this.inputCount);
    }
  }

  public void write(Writer writer) throws IOException {
    this.counter.writeChunks(writer);
  }
}