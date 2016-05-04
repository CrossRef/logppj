package logpp.analyzerstrategies;

import logpp.*;

public class CodeTableAnalyzerStrategy extends ChunkTableAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  public String fileName() {
    return "code.csv";
  }

  // Regex for the kind of files this analyzer wants to see.
  public String getInputFileRegex() {
    return "\\d\\d\\d\\d-\\d\\d\\-code.csv-chunks";
  }
}
