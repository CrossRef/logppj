package logpp.analyzerstrategies;

import logpp.*;

public class DomainAnalyzerStrategy extends ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  public String fileName() {
    return "domain.csv-chunks";
  }

  // Regex for the kind of files this analyzer wants to see.
  public String getInputFileRegex() {
    return "\\d\\d\\d\\d-\\d\\d\\-domain.csv-chunks";
  }

  public int getNumPartitions() {
    return 1;
  }
}
