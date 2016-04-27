package logpp;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

// Count referrer type codes (e.g. https vs https) per day.
class CodeAggregatorStrategy implements AggregatorStrategy {
  // Ignore counts under this value.
  long inputCount = 0;

  // Map of Code, Date string => count.
  HashMap<String, Integer> counter;
  Partitioner partitioner;

  CodeAggregatorStrategy() {
    this.reset();
  }

  public int numPartitions() {
    return 1;
  }

  public String fileName(String date) {
    return String.format("%s-referrer-code",  date);
  }

  public void reset() {
    this.counter = new HashMap<String, Integer>();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  public int partition(String[] line) {
    // No need to partition, the set codes is tiny.
    return 0;
  }

  public void feed(String[] line) {
    // date:code
    String key = line[0] + ":" + line[2];
    this.counter.put(key, this.counter.getOrDefault(key, 0) + 1);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. Frequency table size: %d. \n", this.inputCount, this.counter.size());
    }
  }

  public void write(Writer writer) throws IOException {
    for (Map.Entry<String, Integer> entry : this.counter.entrySet()) {
      Integer count = entry.getValue();
      String[] dateCode = entry.getKey().split(":");
      
      // code
      writer.write(dateCode[1]);
      writer.write("\t");

      // date
      writer.write(dateCode[0]);
      writer.write("\t");

      // count
      writer.write(count.toString());
      writer.write("\n");
    }
  }
}