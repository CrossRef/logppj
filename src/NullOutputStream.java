package logpp;

import java.io.IOException;
import java.io.Writer;

// An output stream that does nothing.
// Used as a marker for 'don't write anything' so isn't ever actually written to anyway.
public  class NullOutputStream extends Writer {
  @Override
  public void write(int b) throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void write(char[] cbuf, int off, int len) {
  }

  @Override
  public void write(char[] cbuf) {
  }
}