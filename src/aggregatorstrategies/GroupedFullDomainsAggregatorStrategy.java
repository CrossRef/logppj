package logpp.aggregatorstrategies;

import logpp.*;


import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

// Group fulldomains by their domain.
// Output as CSV Chunks, though only with one value per line. Header is domain, chunks are fulldomains.
public class GroupedFullDomainsAggregatorStrategy implements AggregatorStrategy {
  // domain -> full domains
  private Map<String, Set<String>> groupedDomains;

  private Partitioner partitioner;

  long inputCount;

  public GroupedFullDomainsAggregatorStrategy() {
    this.partitioner = new Partitioner(this.numPartitions());

    this.reset();
  }

  public String toString() {
    return String.format("GroupedFullDomainsAggregatorStrategy, %d partitions", this.numPartitions());
  }

  public int numPartitions() {
    return 1;
  }

  public String fileName(String date) {
    return String.format("%s-grouped-fulldomain.csv-chunks", date);
  }

  public void reset() {
    this.groupedDomains = new HashMap<>();
    this.inputCount = 0;
  }

  // [date, doi, code, full-domain, subdomains, domain]
  public int partition(String[] line) {
    // Group by the domain
    return this.partitioner.partition(line[5]);
  } 

  // line is [date, doi, code, full-domain, subdomain, domain]
  public void feed(String[] line) {
    String domain = line[5];
    String fulldomain = line[4];

    Set<String> fulldomains = this.groupedDomains.get(domain);
    if (fulldomains == null) {
      fulldomains = new TreeSet<>();
      this.groupedDomains.put(domain, fulldomains);
    }

    fulldomains.add(fulldomain);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. \n", this.inputCount);
    }
  }

  public void write(Writer writer) throws IOException {
    for (Map.Entry<String, Set<String>> domainEntry : this.groupedDomains.entrySet()) {
      String domain = domainEntry.getKey();
      writer.write(domain);
      writer.write("\n");

      for (String fullDomain : domainEntry.getValue()) {
        // If we got an empty string that's because the domain showed up on its own. If so, write it.
        // Otherwise write the full domain, which means building it.
        if (fullDomain.length() == 0) {
          writer.write(domain);
        } else {
          writer.write(fullDomain + "." + domain);
        }
        
        writer.write("\n");
      }

      writer.write("\n");
    }
  }
}