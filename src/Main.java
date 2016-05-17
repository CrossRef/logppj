package logpp;

import java.io.File;
import logpp.aggregatorstrategies.*;
import logpp.analyzerstrategies.*;

import java.util.Arrays;

public class Main {
  static void analyze(String workingDir)  {
    String inputPath = workingDir + "/aggregated";
    String outputPath = workingDir + "/analyzed";

    File input = new File(inputPath);
    File output = new File(outputPath);

    System.out.format("Process %s to %s\n", inputPath, outputPath);
    Analyzer analyzer = new Analyzer(input, output);

    AnalyzerStrategy[] strategies = new AnalyzerStrategy[] {
      // new TopNDomainsAnalyzerStrategy(),
      // new DomainCSVAnalyzerStrategy(),
      // new SubdomainCSVAnalyzerStrategy(),
      // new DOIAnalyzerStrategy()

      // new CodeTableAnalyzerStrategy()
      // new TopNDomainsTableAnalyzerStrategy()
    };

    try {
      for (AnalyzerStrategy strategy: strategies) {
        System.out.format("Analyze with strategy: %s \n", strategy.toString());
        analyzer.run(strategy);
        System.out.format("Finished analyze with strategy: %s \n", strategy.toString());
      }
    } catch (Exception e) {
      System.err.println("Error:");
      e.printStackTrace();
    }
  }

  static void aggregate(String workingDir)  {
    String inputPath = workingDir + "/processed";
    String outputPath = workingDir + "/aggregated";

    File input = new File(inputPath);
    File output = new File(outputPath);

    System.out.format("Process %s to %s\n", inputPath, outputPath);
    Aggregator aggregator = new Aggregator(input, output);

    AggregatorStrategy[] strategies = new AggregatorStrategy[] {
      new DomainCSVAggregatorStrategy(Constants.MODE_DAY),
      new FullDomainCSVAggregatorStrategy(Constants.MODE_MONTH)

      
      // new CodeCSVAggregatorStrategy()
      // new DOICSVAggregatorStrategy()
    };

    try {
      for (AggregatorStrategy strategy: strategies) {
        System.out.format("Aggregate with strategy: %s \n", strategy.toString());
        aggregator.run(strategy);
        System.out.format("Finished aggregate with strategy: %s \n", strategy.toString());
      }
    } catch (Exception e) {
      System.err.println("Error:");
      e.printStackTrace();
    }
  }

  static void preprocess(String workingDir)  {    
    String inputPath = workingDir + "/logs";
    String outputPath = workingDir + "/processed";

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
    System.out.println("Run with " + Arrays.toString(argv));

    if (argv.length < 2) {
      System.err.println("Provide a command and a working directory.");
      System.exit(1);
    }

    String command = argv[0];
    String workingDir = argv[1];

    switch (command) {
      case "pp": preprocess(workingDir); break;
      case "aggregate": aggregate(workingDir); break;
      case "analyze": analyze(workingDir); break;
    }
    
  }
}