package logpp.analyzerstrategies;

import logpp.*;

public class DomainAnalyzerStrategy extends ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  private DateProjector dateProjector;

  public DomainAnalyzerStrategy(DateProjector dateProjector) {
    this.dateProjector = dateProjector;
  }
  
  public String fileName() {
    return String.format("%s-domain.csv-chunks", this.dateProjector.getName());
  }

  // Regex for the kind of files this analyzer wants to see.
  public String getInputFileRegex() {
    return String.format("\\d\\d\\d\\d-\\d\\d\\-%s-domain.csv-chunks", this.dateProjector.getName());
  }

  public int getNumPartitions() {
    return 1;
  }
}
