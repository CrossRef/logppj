package logpp;

import java.io.IOException;

public interface DistributerStrategy {
  public void run() throws IOException;
}