package logpp;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.io.Writer;

// Count unique values for String -> int.
// e.g. date -> count
public class Counter1d {
  private TreeMap<String, int[]> counter = new TreeMap<String, int[]>();

  public void inc(String key) {
    int[] count = this.counter.get(key);
    if (count == null) {
      this.counter.put(key, new int[] {1});
    } else {
      count[0]++;
    }
  }

  // Write CSV.
  public void write(Writer writer) throws IOException {
    for (SortedMap.Entry<String, int[]> entry : this.counter.entrySet()) {
      writer.write(entry.getKey());
      writer.write(",");
      writer.write(Integer.toString(entry.getValue()[0]));
      writer.write("\n");
    }
  }
}