package logpp.aggregatorstrategies;

import logpp.*;


import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

// Count referring domain name per day.
// Output as CSV Chunks.
public class DomainCSVAggregatorStrategy implements AggregatorStrategy {
  // Used to project the date for aggregation, e.g. truncate to the month.
  private DateProjector dateProjector;

  private long inputCount = 0;

  // Domain -> date => count.
  private Counter2d counter = new Counter2d();

  private Partitioner partitioner;

  public DomainCSVAggregatorStrategy(DateProjector dateProjector) {
    this.dateProjector = dateProjector;
    this.partitioner = new Partitioner(this.numPartitions());

    this.reset();
  }

  public String toString() {
    return String.format("DomainCSVAggregatorStrategy, %d partitions", this.numPartitions());
  }

  // Different projection modes have different amounts of data to store, so smaller partition sizes required for "day".
  public int numPartitions() {
    switch (this.dateProjector.getName()) {
      case "month": return 2; 
      case "day": return 5; 
      default: return 10; 
    }
  }

  // Filename depends on the date projection (day or month).
  public String fileName(String date) {
    return String.format("%s-%s-domain.csv-chunks", date, this.dateProjector.getName());
  }

  public void reset() {
    this.counter = new Counter2d();
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