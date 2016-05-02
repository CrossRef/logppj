package logpp.aggregatorstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

// Count referring type code per day.
// Output as:
// »code»
// «date» «count»
// «date» «count»
// «date» «count»
// ...
// «blank line»
// «repeat»
public class CodeCSVAggregatorStrategy implements AggregatorStrategy {
  long inputCount = 0;

  // Map of Code => Date string => count.
  HashMap<String, Map<String, Integer>> counter;

  Partitioner partitioner;

  public CodeCSVAggregatorStrategy() {
    this.reset();
  }

  public String toString() {
    return String.format("CodeCSVAggregatorStrategy, %d partitions", this.numPartitions());
  }

  public int numPartitions() {
    return 1;
  }

  public String fileName(String date) {
    return String.format("%s-code.csv-chunks",  date);
  }

  public void reset() {
    this.counter = new HashMap<String, Map<String, Integer>>();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  public int partition(String[] line) {
    return 0;
  } 

  // line is [date, doi, code, domain]
  public void feed(String[] line) {
    String code = line[2];
    String date = line[0];

    Map<String, Integer> dateCounter = this.counter.get(code);
    if (dateCounter == null) {
      dateCounter = new HashMap<String, Integer>();
      this.counter.put(code, dateCounter);
    }
    dateCounter.put(date, dateCounter.getOrDefault(date, 0) + 1);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. Frequency table size: %d. \n", this.inputCount, this.counter.size());
    }
  }

  public void write(Writer writer) throws IOException {
    for (Map.Entry<String, Map<String, Integer>> codeEntry : this.counter.entrySet()) {
      writer.write(codeEntry.getKey());
      writer.write("\n");

      for (Map.Entry<String, Integer> dateEntry : codeEntry.getValue().entrySet()) {
        writer.write(dateEntry.getKey());
        writer.write(",");
        writer.write(dateEntry.getValue().toString());
        writer.write("\n");
      }

      writer.write("\n");
    }
  }
}
