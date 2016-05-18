package logpp.analyzerstrategies;

import logpp.*;

public class CodeTableAnalyzerStrategy extends ChunkTableAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  private DateProjector dateProjector;

  public CodeTableAnalyzerStrategy(DateProjector dateProjector) {
    this.dateProjector = dateProjector;
  }

  public String fileName() {
    return String.format("%s-code.csv", this.dateProjector.getName());
  }

  // Regex for the kind of files this analyzer wants to see.
  public String getInputFileRegex() {
    return String.format("\\d\\d\\d\\d-\\d\\d\\-%s-code.csv-chunks", this.dateProjector.getName());
  }
}
