package logpp.analyzerstrategies;

import logpp.*;

public class FullDomainAnalyzerStrategy extends ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  private DateProjector dateProjector;

  public FullDomainAnalyzerStrategy(DateProjector dateProjector) {
    this.dateProjector = dateProjector;
  }

  public String fileName() {
    return String.format("%s-fulldomain.csv-chunks", this.dateProjector.getName());
  }

  // Regex for the kind of files this analyzer wants to see.
  public String getInputFileRegex() {
    return String.format("\\d\\d\\d\\d-\\d\\d\\-%s-fulldomain.csv-chunks", this.dateProjector.getName());
  }

  public int getNumPartitions() {
    return 1;
  }
}
