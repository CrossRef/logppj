package logpp.analyzerstrategies;

import logpp.*;

// Glom all chunks per Code together to give all dates per code.
// Don't try and parse the CSV lines within each chunk.
public class CodeAnalyzerStrategy extends ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  public String fileName() {
    return "code.csv-chunks";
  }

  // Regex for the kind of files this analyzer wants to see.
  public String getInputFileRegex() {
    return "\\d\\d\\d\\d-\\d\\d\\-code.csv-chunks";
  }

  public int getNumPartitions() {
    return 1;
  }
}
