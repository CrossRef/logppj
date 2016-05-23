package logpp;

import java.io.IOException;
import java.io.Writer;


// A particular kind of analysis.
// This is a stateful strategy object with various callbacks. 
// Not threadsafe.
public interface AnalyzerStrategy {
  void assignOutputFile(Writer writer);

  // Filename for this kind out output.
  String fileName();

  // Regex for the kind of files this analyzer wants to see.
  String getInputFileRegex();

  // How many partitions does this analyzer want?
  int getNumPartitions();

  // Called between partitions.
  void enterPartition(int partitionNumber) throws IOException;

  // Process the line.
  // Depends on type of analysis, up to the strategy.
  void feed(String line);

  // Write everything to the output file.
  void finish() throws IOException;

  // Let go of unwanted memory. Object can't be used after this.
  void dispose();
}
