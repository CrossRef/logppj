package logpp.analyzerstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.SortedMap;
import java.util.LinkedList;
import java.util.Collection;

// We're into 32-bit int overflow territory with some of the count sums.

class DateCountEntry implements Comparable<DateCountEntry> {
  String entryName;
  Long count;

  public Long getCount() {
    return this.count;
  }

  public String getEntryName() {
    return this.entryName;
  }

  public int compareTo(DateCountEntry other) {
    return Long.compare(this.count, other.getCount());
  }

  DateCountEntry(String entryName, Long count) {
    this.entryName = entryName;
    this.count = count;
  }

  public String toString() {
    return entryName + ": " + count;
  }
}

class DateEntry {
  int limit;
  String date;
  Long smallestValue = 0L;
  List<DateCountEntry> entries;

  public DateEntry(int limit, String date) {
    this.limit = limit;
    this.date = date;
    this.entries = new ArrayList<>();
  }

  public void add(String entryName, Long count) {
    this.entries.add(new DateCountEntry(entryName, count));
    
    // Expand and vacuuum.
    if (this.entries.size() >= this.limit * 20) {
      this.vacuum(limit);
    }
  }

  public Collection<String> topNEntries(int n) {
    Collections.sort(this.entries, Collections.reverseOrder());
    Collection<String> entryNames = new ArrayList<String>();

    for (DateCountEntry entry: this.entries.subList(0, n)) {
      entryNames.add(entry.getEntryName());
    }

    return entryNames;
  }

  public Map<String, Long> getEntriesDictionary() {
    Map<String, Long> map = new HashMap<>();

    for (DateCountEntry entry: this.entries) {
      map.put(entry.getEntryName(), entry.getCount());
    }

    return map;
  }

  public void vacuum(int limitTo) {
    Collections.sort(this.entries, Collections.reverseOrder());
    this.entries = this.entries.subList(0, limitTo);
  }
}

public abstract class TopNDomainsTableAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  // All dates we've seen.
  private long counter = 0;

  private ChunkParser chunkParser = new ChunkParser(this);

  // Date => DateEntry, sorted by date.
  private SortedMap<String, DateEntry> dateEntries = new TreeMap<>();

  // Name of current chunk.
  // Can be null if we're ignoring the current chunk.
  private String currentChunkHeader;

  public abstract String fileName();

  public abstract String getInputFileRegex();

  Writer outputFile;

  // Used to decide if a domain should be ignored. Can be null.
  protected DomainIgnorer ignorer;

  public TopNDomainsTableAbstractStrategy(DomainIgnorer ignorer) {
    this.ignorer = ignorer;
  }

  public void assignOutputFile(Writer writer) {
    this.outputFile = writer;
  }

  public int getNumPartitions() {
    return 1;
  }

  // Initial filter top N per month.
  public abstract int preN();

  // Final top N for all months.
  public abstract int finalN();

  private void reset() throws IOException {
    // Reset doesn't change anything.
  }

  public void enterPartition(int partitionNumber) throws IOException {
    // Only one partition.
  }

  public void finish() throws IOException {
    this.write();
    this.outputFile.flush();
  }

  // Process the line.
  public void feed(String line) {
    chunkParser.feed(line);
  }

  // Write everything to the output file.
  public void write() throws IOException {
    // Find the domains that occur in the union of the top N domains for any month.
    SortedSet<String> topEntryNames = new TreeSet<>();
    Iterator<Map.Entry<String, DateEntry>> dateEntriesIterator = this.dateEntries.entrySet().iterator();
    while (dateEntriesIterator.hasNext()) {
      Map.Entry<String, DateEntry> dateEntry = dateEntriesIterator.next();
      topEntryNames.addAll(dateEntry.getValue().topNEntries(this.finalN()));
    }

    // Now sort them by their all-time total.
    // Collect totals.
    Map<String, Long> topDomainSums = new HashMap<>();
    for (Map.Entry<String, DateEntry> dateEntry : this.dateEntries.entrySet()) {
      Map<String, Long> domainEntries = dateEntry.getValue().getEntriesDictionary();

      for (String domain : topEntryNames) {
        topDomainSums.put(domain, topDomainSums.getOrDefault(domain, 0L) + domainEntries.getOrDefault(domain, 0L));
      }
    }

    // Sort domains by their totals.
    List<WeightedString> sortedDomains = new ArrayList<>();
    for (Map.Entry<String, Long> domainSum : topDomainSums.entrySet()) {
      sortedDomains.add(new WeightedString(domainSum.getKey(), domainSum.getValue()));
    }
    Collections.sort(sortedDomains, Collections.reverseOrder());

    // Write sorted domains header
    this.outputFile.write("Date");
    this.outputFile.write(",");

    // Iterate over the domains sorted by total.
    Iterator<WeightedString> entryNamesIterator = sortedDomains.iterator();
    while(entryNamesIterator.hasNext()) {
      String entryName = entryNamesIterator.next().getValue();
      this.outputFile.write(entryName);

      if (entryNamesIterator.hasNext()) {
        this.outputFile.write(",");
      }
    }
    this.outputFile.write("\n");

    // Now write counts for only those top entry names for each date.
    dateEntriesIterator = this.dateEntries.entrySet().iterator();
    while (dateEntriesIterator.hasNext()) {
      Map.Entry<String, DateEntry> dateEntryName = dateEntriesIterator.next();
      String date = dateEntryName.getKey();
      DateEntry entry = dateEntryName.getValue();
      Map<String, Long> entryCounts = entry.getEntriesDictionary();
      
      this.outputFile.write(date);
      this.outputFile.write(",");

      entryNamesIterator = sortedDomains.iterator();
      while(entryNamesIterator.hasNext()) {
        String entryName = entryNamesIterator.next().getValue();

        Long count = entryCounts.get(entryName);
        if (count == null) {
          // System.err.format("ERROR failed to fetch %s on %s. Increase preN.\n", entryName, date);
          count = 0L;
        }
        
        this.outputFile.write(count.toString());

        if (entryNamesIterator.hasNext()) {
          this.outputFile.write(",");
        }
      }

      this.outputFile.write("\n");
    }
  }

  // ChunkParserCallback
  public void header(String name) {
    // If we have an ignorer in operation, ignore if it wants us to. 
    // If there isn't one, set it.
    if (this.ignorer != null && this.ignorer.ignore(name)) {
      this.currentChunkHeader = null;
    } else {
      this.currentChunkHeader = name;
    }
  }

  // ChunkParserCallback
  public void line(String line) { 

    // If we're ignoring the chunk header, do nothing.
    if (this.currentChunkHeader == null) {
      return;
    }

    this.counter++;
    if (this.counter % 1000000 == 0) {
      System.out.println(counter);
    }

    String[] dateCount = line.split(",");
    String date = dateCount[0];
    Long count = Long.parseLong(dateCount[1]);

    DateEntry entry = this.dateEntries.get(date);
    if (entry == null) {
      entry = new DateEntry(this.preN(), date);
      this.dateEntries.put(date, entry);
    }

    entry.add(this.currentChunkHeader, count);
  }
}
