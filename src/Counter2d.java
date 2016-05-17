package logpp;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.io.Writer;

// Count unique values for String -> String -> Integer.
// e.g. domain -> date -> count
public class Counter2d {
  SortedMap<String, SortedMap<String, Integer>> counter = new TreeMap<>();

  public void inc(String key, String key2) {
    this.add(key, key2, 1);
  }

  public void add(String key, String key2, Integer value) {
    SortedMap<String, Integer> second = this.counter.get(key);
    if (second == null) {
      second = new TreeMap<String, Integer>();
      this.counter.put(key, second);
    }
    
    second.put(key2, second.getOrDefault(key2, 0) + value);
  }

  public SortedMap<String, Integer> get(String key) {
    return this.counter.get(key);
  }

  public void writeChunks(Writer writer) throws IOException {
    for (SortedMap.Entry<String, SortedMap<String, Integer>> entry : this.counter.entrySet()) {
      writer.write(entry.getKey());
      writer.write("\n");

      for (SortedMap.Entry<String, Integer> secondEntry : entry.getValue().entrySet()) {
        writer.write(secondEntry.getKey());
        writer.write(",");
        writer.write(secondEntry.getValue().toString());
        writer.write("\n");
      }

      writer.write("\n");
    }
  }
}