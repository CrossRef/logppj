package logpp.aggregatorstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

// Count referring type code per day.
// Output as CSV Chunks.
public class CodeCSVAggregatorStrategy implements AggregatorStrategy {
  // How to truncate dates.
  private DateProjector dateProjector;

  // code -> date -> count
  private Counter2d counter;

  private long inputCount = 0;

  private Partitioner partitioner;

  public CodeCSVAggregatorStrategy(DateProjector dateProjector) {
    this.dateProjector = dateProjector;
    this.partitioner = new Partitioner(this.numPartitions());
    this.reset();
  }

  public String toString() {
    return String.format("CodeCSVAggregatorStrategy, %d partitions", this.numPartitions());
  }

  // We only need one pass at this, the domain of codes is very small.
  public int numPartitions() {
    return 1;
  }

  // Filename depends on the date projection (day or month).
  public String fileName(String date) {
    return String.format("%s-%s-code.csv-chunks", date, this.dateProjector.getName());
  }

  public void reset() {
    this.counter = new Counter2d();
    this.inputCount = 0;
  }

  // Eternally in partition 0. There is only one to choose from.
  public int partition(String[] line) {
    return 0;
  } 

  // line is [date, doi, code, full-domain, subdomains, domain]
  public void feed(String[] line) {
    String code = line[2];
    String date = line[0];

    this.counter.inc(code, this.dateProjector.project(date));

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines.. \n", this.inputCount);
    }
  }

  public void write(Writer writer) throws IOException {
    this.counter.writeChunks(writer);
  }
}
