package logpp.analyzerstrategies;

import logpp.*;

public class GroupedFullDomainsAnalyzerStrategy extends ChunkGlommerAbstractStrategy implements AnalyzerStrategy, ChunkParserCallback {
  public GroupedFullDomainsAnalyzerStrategy(Filter filter) {
    super(filter);
  }

  public int getNumPartitions() {
    return 1;
  }

  public String fileName() {
    return String.format("%s-grouped-fulldomain.csv", this.filter.getName());
  }

  public String getInputFileRegex() {
    return String.format("\\d\\d\\d\\d-\\d\\d\\-grouped-fulldomain.csv-chunks");
  }
}
