package logpp.aggregatorstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

// Count referring domain name per day.
// Output as unordered «domain» «date» «count».
public class DomainAggregatorStrategy implements AggregatorStrategy {
  // Ignore counts under this value.
  static int CUTOFF = 10;
  long inputCount = 0;

  IdentityMap domainIds;

  // Map of Domain ID, Date string => count.
  HashMap<String, Integer> counter;
  Partitioner partitioner;

  public DomainAggregatorStrategy() {
    this.reset();
  }

  public String toString() {
    return String.format("DomainAggregatorStrategy, %d partitions", this.numPartitions());
  }


  public int numPartitions() {
    return 20;
  }

  public String fileName(String date) {
    return String.format("%s-domain",  date);
  }

  public void reset() {
    this.counter = new HashMap<String, Integer>();
    this.domainIds = new IdentityMap();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  public int partition(String[] line) {
    // Date is evenly distributed but there aren't many. 
    // Use domain.

    
    return this.partitioner.partition(line[1]);
  } 

  // line is [date, doi, code, domain]
  public void feed(String[] line) {
    Integer domainId = this.domainIds.get(line[3]);
    // date:domainId
    String key = line[0] + ":" + domainId;

    this.counter.put(key, this.counter.getOrDefault(key, 0) + 1);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. Frequency table size: %d. Identity map size %d \n", this.inputCount, this.counter.size(), this.domainIds.count());
    }
  }

  public void write(Writer writer) throws IOException {
    for (Map.Entry<String, Integer> entry : this.counter.entrySet()) {

      Integer count = entry.getValue();
      if (count > CUTOFF) {
        String[] dateDomain = entry.getKey().split(":");
        String date = dateDomain[0];

        String domain = this.domainIds.getInverse(Integer.parseInt(dateDomain[1]));

        writer.write(domain);
        writer.write("\t");

        writer.write(date);
        writer.write("\t");

        writer.write(count.toString());
        writer.write("\n");
      }
    }
  }
}