package logpp.analyzerstrategies;

import logpp.*;

import java.io.Writer;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;


// Create files of:
// full-domain
// domain
// «blank»
public class FullDomainDomainAnalyzerStrategy implements AnalyzerStrategy, ChunkParserCallback {
  private ChunkParser chunkParser = new ChunkParser(this);
  Writer outputFile;

  // We just collect fulldomain -> domain.
  Map<String, String> fulldomainDomain = new HashMap<>();
  String currentHeader;

  public void assignOutputFile(Writer outputFile) {
    this.outputFile = outputFile;
  }

  public void feed(String line) {
    this.chunkParser.feed(line);
  }

  public int getNumPartitions() {
    return 1;
  }

  public void enterPartition(int partitionNumber) {
    // pass
  }

  public String fileName() {
    return String.format("fulldomain-domain.csv-chunks");
  }

  public String getInputFileRegex() {
    return String.format("\\d\\d\\d\\d-\\d\\d\\-grouped-fulldomain.csv-chunks");
  }

  public void finish() throws IOException {
    for (Map.Entry<String, String> entry: this.fulldomainDomain.entrySet()) {
      this.outputFile.write(entry.getKey());
      this.outputFile.write("\n");
      this.outputFile.write(entry.getValue());
      this.outputFile.write("\n\n");
    }
    this.outputFile.flush();
  }

  // From ChunkParserCallback
  public void line(String line) {
    if (currentHeader.length() == 0) {
      System.exit(1);
    }
    this.fulldomainDomain.put(line, this.currentHeader);
  }

  public void header(String line) {
    this.currentHeader = line;
  }

}
