package logpp;

// Event callback for ChunkParser. 
public interface ChunkParserCallback {
  // Seen a header.
  void header(String name);

  // Seen a line.
  void line(String fields);
}