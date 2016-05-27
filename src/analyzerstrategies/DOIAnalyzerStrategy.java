package logpp.analyzerstrategies;

import logpp.*;

// Glom all chunks per DOI together to give all dates per DOI.
// Don't try and parse the CSV lines within each chunk.
public class DOIAnalyzerStrategy extends ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  public DOIAnalyzerStrategy() {
    // No filter for DOIs.
    super(new EverythingFilter());
  }

  public String fileName() {
    return "day-doi.csv-chunks";
  }

  // Regex for the kind of files this analyzer wants to see.
  public String getInputFileRegex() {
    return "\\d\\d\\d\\d-\\d\\d\\-day-doi.csv-chunks";
  }

  public int getNumPartitions() {
    return 10;
  }
}
