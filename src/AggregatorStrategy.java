package logpp;

import java.io.IOException;
import java.io.Writer;

// A particular kind of aggregation.
// This is a stateful strategy object with various callbacks. 
// Not threadsafe.
public interface AggregatorStrategy {
  // How many partitions required.
  // Based on observed heuristics.
  int numPartitions();

  // Filename for this kind out output based on date.
  String fileName(String date);

  // Rather than have another layer of context objects and context object factories, allow reset of state between runs.
  void reset();

  // Which partition does this line belong to?
  int partition(String[] line);

  // Process the line.
  // Line is [date, doi, code, domain].
  void feed(String[] line);

  // Write everything to the output file.
  void write(Writer writer) throws IOException;
}
