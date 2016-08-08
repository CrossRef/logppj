package logpp.aggregatorstrategies;

import logpp.*;


import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

// Just count everything.
// Output as CSV Chunks.
public class AllCountCSVAggregatorStrategy implements AggregatorStrategy {
  // Used to project the date for aggregation, e.g. truncate to the month.
  private DateProjector dateProjector;

  private long inputCount = 0;

  // Date => count.
  private Counter1d counter = new Counter1d();

  private Partitioner partitioner;

  public AllCountCSVAggregatorStrategy(DateProjector dateProjector) {
    this.dateProjector = dateProjector;
    this.partitioner = new Partitioner(this.numPartitions());

    this.reset();
  }

  public String toString() {
    return String.format("AllCountCSVAggregatorStrategy, %d partitions", this.numPartitions());
  }

  // No need to partition for this, we're collecting a very small number of items.
  public int numPartitions() {
    return 1;
  }

  // Filename depends on the date projection (day or month).
  public String fileName(String date) {
    return String.format("%s-%s-all.csv-chunks", date, this.dateProjector.getName());
  }

  public void reset() {
    this.counter = new Counter1d();
    this.inputCount = 0;
  }

  // [date, doi, code, full-domain, subdomains, domain]
  public int partition(String[] line) {
    // Only ever one partition. 
    return 0;
  } 

  // line is [date, doi, code, full-domain, subdomain, domain]
  public void feed(String[] line) {
    String date = line[0];

    this.counter.inc(date);

    // this.counter.inc(domain, this.dateProjector.project(date));

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. \n", this.inputCount);
    }
  }

  public void write(Writer writer) throws IOException {
    this.counter.write(writer);
  }
}