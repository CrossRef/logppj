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

// Collect data into a 2d table.
// Only for small things like Referrer Code.
public abstract class ChunkTableAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  // All dates we've seen.
  SortedSet<String> dates = new TreeSet<>();

  // All the chunk headers we've seen.
  SortedSet<String> headers = new TreeSet<>();

  // Makes callbacks on this.
  private ChunkParser chunkParser = new ChunkParser(this);

  // Date => Header => Count
  private Map<String, Map<String, String>> dateHeaderCount = new HashMap<>();

  // Name of current chunk.
  private String currentChunkHeader;

  public abstract String fileName();

  public abstract String getInputFileRegex();

  Writer outputFile;

  public void assignOutputFile(Writer writer) {
    this.outputFile = writer;
  }

  public int getNumPartitions() {
    return 1;
  }

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
    this.outputFile.write("Date");
    this.outputFile.write(",");
    Iterator<String> headersIterator = this.headers.iterator();
    while (headersIterator.hasNext()) {
      String header = headersIterator.next();
      this.outputFile.write(header);

      if (headersIterator.hasNext()) {
        this.outputFile.write(",");
      }
    }

    this.outputFile.write("\n");

    Iterator<String> datesIterator = this.dates.iterator();
    String date;
    while (datesIterator.hasNext()) {
      date = datesIterator.next();

      Map<String, String> headerCount = this.dateHeaderCount.get(date);

      this.outputFile.write(date);
      this.outputFile.write(",");

      String header;
      headersIterator = this.headers.iterator();
      while (headersIterator.hasNext()) {
        header = headersIterator.next();

        this.outputFile.write(headerCount.getOrDefault(header, "0"));
        
        if (headersIterator.hasNext()) {
          this.outputFile.write(",");
        }
      }

      this.outputFile.write("\n");
    }
  }

  // ChunkParserCallback
  public void header(String name) {
    this.currentChunkHeader = name;
    this.headers.add(name);
  }

  // ChunkParserCallback
  public void line(String line) {    
    String[] dateCount = line.split(",", -1);
    this.dates.add(dateCount[0]);

    Map<String, String> itemCounts = this.dateHeaderCount.get(dateCount[0]);

    if (itemCounts == null) {
      itemCounts = new HashMap<String, String>();
      this.dateHeaderCount.put(dateCount[0], itemCounts);
    }

    itemCounts.put(currentChunkHeader, dateCount[1]);
  }
}
