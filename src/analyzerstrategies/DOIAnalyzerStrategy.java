package logpp.analyzerstrategies;

import logpp.*;

// Glom all chunks per DOI together to give all dates per DOI.
// Don't try and parse the CSV lines within each chunk.
public class DOIAnalyzerStrategy extends ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  public DOIAnalyzerStrategy(Filter filter) {
    super(filter);
  }

  public String fileName() {
    return "doi.csv-chunks";
  }

  // Regex for the kind of files this analyzer wants to see.
  public String getInputFileRegex() {
    return "\\d\\d\\d\\d-\\d\\d\\-doi.csv-chunks";
  }

  public int getNumPartitions() {
    return 10;
  }
}
