package logpp.aggregatorstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

// Count DOI per day.
public class DOIAggregatorStrategy implements AggregatorStrategy {
  // Ignore counts under this value.
  static int CUTOFF = 10;
  long inputCount = 0;

  IdentityMap doiIds;

  // Map of DOI ID, Date string => count.
  HashMap<String, Integer> counter;
  Partitioner partitioner;

  public DOIAggregatorStrategy() {
    this.reset();
  }

  public String toString() {
    return String.format("DOIAggregatorStrategy, %d partitions", this.numPartitions());
  }


  public int numPartitions() {
    return 100;
  }

  public String fileName(String date) {
    return String.format("%s-doi",  date);
  }

  public void reset() {
    this.counter = new HashMap<String, Integer>();
    this.doiIds = new IdentityMap();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  public int partition(String[] line) {
    // Date is evenly distributed but there aren't many. DOI is more suitable.
    return this.partitioner.partition(line[1]);
  }

  // Line is [date, doi, code, full-domain, subdomains, domain]
  public void feed(String[] line) {
    Integer doiId = this.doiIds.get(line[1]);
    // date:doi
    String key = line[0] + ":" + doiId;
    this.counter.put(key, this.counter.getOrDefault(key, 0) + 1);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. Frequency table size: %d. Identity map size %d \n", this.inputCount, this.counter.size(), this.doiIds.count());
    }
  }

  public void write(Writer writer) throws IOException {
    for (Map.Entry<String, Integer> entry : this.counter.entrySet()) {
      Integer count = entry.getValue();
      if (count > CUTOFF) {
        String[] dateDoi = entry.getKey().split(":");
        String date = dateDoi[0];

        String doi = this.doiIds.getInverse(Integer.parseInt(dateDoi[1]));

        writer.write(doi);
        writer.write("\t");

        writer.write(date);
        writer.write("\t");

        writer.write(count.toString());
        writer.write("\n");
      }
    }
  }
}
