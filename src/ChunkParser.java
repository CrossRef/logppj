package logpp;

// Event-based parser for a CSV Chunk file. Make callbacks.
public class ChunkParser {
  String currentHeader = null;
  ChunkParserCallback callback = null;

  public ChunkParser(ChunkParserCallback callback) {
    this.callback = callback;
  }

  public void feed(String line) {
    // Expect header name.
    if (this.currentHeader == null) {
      this.currentHeader = line;
      this.callback.header(line);
    // Or see if we're at the end of a block.
    } else if (line.length() == 0) {
      this.currentHeader = null;
    // Or it's a data value.
    } else {
      // pass
      this.callback.line(line);
    }
  }
  
}