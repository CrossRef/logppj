package logpp;

import java.io.File;

public class Main {
  static void aggregate(String[] argv)  {
    if (argv.length != 3) {
      System.err.println("Both input and output path must be supplied");
      System.exit(1);
    }

    String inputPath = argv[1];
    String outputPath = argv[2];

    File input = new File(inputPath);
    File output = new File(outputPath);

    System.out.format("Process %s to %s\n", inputPath, outputPath);
    Aggregator aggregator = new Aggregator(input, output);

    // AggregatorStrategy strategy = new DOIAggregatorStrategy();
    AggregatorStrategy strategy = new CodeAggregatorStrategy();

    try {
      aggregator.run(strategy);
    } catch (Exception e) {
      System.err.println("Error:");
      e.printStackTrace();
    }
  }

  static void preprocess(String[] argv)  {
    if (argv.length != 3) {
      System.err.println("Both input and output path must be supplied");
      System.exit(1);
    }
    
    String inputPath = argv[1];
    String outputPath = argv[2];

    System.out.println("Input directory: " + inputPath);
    System.out.println("Output directory: " + outputPath);

    File inputDir = new File(inputPath);
    File outputDir = new File(outputPath);

    Parser parser = new Parser(inputDir, outputDir);

    // This designed for supervised use. Don't try to recover.
    try {
      parser.run();
    } catch (Exception e) {
      System.err.println("Error:");
      e.printStackTrace();
    }
  }

  public static void main(String[] argv) {
    String command = argv[0];

    if (argv.length < 1) {
      System.err.println("Provide at least a command.");
      System.exit(1);
    }

    switch (argv[0]) {
      case "pp": preprocess(argv); break;
      case "aggregate": aggregate(argv); break;
    }
    
  }
}