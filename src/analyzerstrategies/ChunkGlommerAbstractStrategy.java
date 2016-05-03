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

// Glom all chunks per domain together.
// All fits in memory. 
// Don't try and parse the CSV lines within each chunk.
public abstract class ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  private Partitioner partitioner = new Partitioner(this.getNumPartitions());

  private ChunkParser chunkParser = new ChunkParser(this);

  private Map<String, List<String>> collection;

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

    for (Map.Entry<String, List<String>> headerEntry : this.collection.entrySet()) {
      // Sort dates within the domain chunk.
      Collections.sort(headerEntry.getValue());

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
    this.currentChunkHeader = name;
    this.chunkCount++;

    this.interestedInChunk = (this.partitioner.partition(name) == this.currentPartitionNumber);
    

    // System.out.format("Header %s interested? %s \n", name, this.interestedInChunk);
  }

  // ChunkParserCallback
  public void line(String line) {
    if (!this.interestedInChunk) {
      return;
    }

    if (this.collection.get(this.currentChunkHeader) == null) {
      List<String> entry = new ArrayList<>();
      entry.add(line);
      this.collection.put(this.currentChunkHeader, entry);
    } else {
      this.collection.get(this.currentChunkHeader).add(line);
    }
  }
}
