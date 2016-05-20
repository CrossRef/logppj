package logpp;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.IOException;

public class CopyDistributerStrategy implements DistributerStrategy {
  private String filename;
  private String inputDir;
  private String outputDir;

  public CopyDistributerStrategy(String inputDir, String outputDir, String filename) {
    this.filename = filename;
    this.inputDir = inputDir;
    this.outputDir = outputDir;
  }

  public void run() throws IOException {
    Path input = Paths.get(inputDir, this.filename);
    Path output = Paths.get(outputDir, this.filename);
    System.out.format("Copy %s to %s\n", input, output);

    Files.deleteIfExists(output);
    Files.copy(input, output);
  }
}