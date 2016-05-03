package logpp;

public interface ChunkParserCallback {
  void header(String name);
  void line(String fields);
}