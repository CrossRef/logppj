package logpp;

import java.util.HashMap;
import java.util.Map;

// Map of strings to integers.
// Not threasafe but doesn't have to be.
class IdentityMap {
  Integer counter = 0;
  Map<String, Integer> entries = new HashMap<String, Integer>();
  Map<Integer, String> inverseEntries = new HashMap<Integer, String>();

  // Get mapping, creating ID if necessary.
  Integer get(String input) {
    Integer entry = this.entries.get(input);
    if (entry != null) {
      return entry;
    }

    counter++;
    
    this.entries.put(input, counter);
    this.inverseEntries.put(counter, input);

    // System.out.println("GET:" + input + " = " + counter);

    return counter;
  }

  // Get inverse mapping, returning null if not found.
  String getInverse(Integer input) {
    return this.inverseEntries.get(input);
  }

  Integer count() {
    return this.counter;
  }
}
