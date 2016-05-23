package logpp.analyzerstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

// Take a load of CSV Chunk files and combine into one file, combining the sections over files.
// Don't try and parse the CSV lines within each chunk.
public abstract class ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  private Partitioner partitioner = new Partitioner(this.getNumPartitions());

  // Filter headers.
  protected Filter filter;

  // Makes callbacks on this.
  private ChunkParser chunkParser = new ChunkParser(this);

  // header (e.g. domain name) -> lines within chunks with that header. 
  // They'll always be unique either in the input (e.g. date,count pairs) or we'll want to uniqufiy them (e.g. union the set of domains).
  private Map<String, SortedSet<String>> collection;

  // Name of current chunk.
  private String currentChunkHeader;

  private int currentPartitionNumber;

  // Are we interested in this chunk?
  // Might not be if we're not interested in the partition. 
  private boolean interestedInChunk = false;

  public abstract String fileName();

  public abstract String getInputFileRegex();

  private long lineCount = 0;
  private long chunkCount = 0;

  Writer outputFile;

  public ChunkGlommerAbstractStrategy(Filter filter) {
    this.filter = filter;
  }

  public void assignOutputFile(Writer writer) {
    this.outputFile = writer;
  }

  public abstract int getNumPartitions();

  private void reset() throws IOException {
    this.write();
    this.outputFile.flush();
    this.collection = new HashMap<>();
  }

  public void enterPartition(int partitionNumber) throws IOException {
    System.out.format("Partition %d \n", partitionNumber);
    this.reset();
    this.currentPartitionNumber = partitionNumber;
  }

  public void finish() throws IOException {
    this.reset();
  }

  // Process the line.
  public void feed(String line) {
    chunkParser.feed(line);
    this.lineCount ++;

    if (this.lineCount % 10000000 == 0) {
      System.out.format("Processed %d lines, %d chunks. Got %d output chunks \n", this.lineCount, this.chunkCount, this.collection.size());
    }
  }

  // Write everything to the output file.
  public void write() throws IOException {
    // First time round won't have one.
    if (this.collection == null) {
      return;
    }

    for (Map.Entry<String, SortedSet<String>> headerEntry : this.collection.entrySet()) {
      this.outputFile.write(headerEntry.getKey());
      this.outputFile.write("\n");
      for (String dateEntry : headerEntry.getValue()) {
        this.outputFile.write(dateEntry);
        this.outputFile.write("\n");
      }
      this.outputFile.write("\n");
    }
  }

  // ChunkParserCallback
  public void header(String name) {
    if (this.filter.keep(name)) {
      this.currentChunkHeader = name;
      this.chunkCount++;
      this.interestedInChunk = (this.partitioner.partition(name) == this.currentPartitionNumber);
    } else {
      this.currentChunkHeader = null;
      this.interestedInChunk = false;
    }
  }

  // ChunkParserCallback
  public void line(String line) {
    if (!this.interestedInChunk) {
      return;
    }

    if (this.collection.get(this.currentChunkHeader) == null) {
      SortedSet<String> entry = new TreeSet<>();
      entry.add(line);
      this.collection.put(this.currentChunkHeader, entry);
    } else {
      this.collection.get(this.currentChunkHeader).add(line);
    }
  }

  public void dispose() {
    this.collection = null;
  }
}
