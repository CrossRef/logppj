package logpp;

import java.io.File;
import logpp.aggregatorstrategies.*;
import logpp.analyzerstrategies.*;

import java.util.Arrays;

public class Main {

  // Run all the analyses.
  static void analyze(String workingDir)  {
    String inputPath = workingDir + "/aggregated";
    String outputPath = workingDir + "/analyzed";

    File input = new File(inputPath);
    File output = new File(outputPath);

    System.out.format("Process %s to %s\n", inputPath, outputPath);
    Analyzer analyzer = new Analyzer(input, output);

    AnalyzerStrategy[] strategies = new AnalyzerStrategy[] {
      new CodeTableAnalyzerStrategy(new TruncateDay()),
      new CodeTableAnalyzerStrategy(new TruncateMonth()),
      new FullDomainAnalyzerStrategy(new TruncateMonth()),
      new DomainAnalyzerStrategy(new TruncateDay()),
      
      new TopNDomainsTableAnalyzerStrategy(new TruncateDay()),
      new TopNDomainsTableAnalyzerStrategy(new TruncateMonth())
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

  // Run all the aggregations.
  static void aggregate(String workingDir)  {
    String inputPath = workingDir + "/processed";
    String outputPath = workingDir + "/aggregated";

    File input = new File(inputPath);
    File output = new File(outputPath);

    System.out.format("Process %s to %s\n", inputPath, outputPath);
    Aggregator aggregator = new Aggregator(input, output);

    AggregatorStrategy[] strategies = new AggregatorStrategy[] {
      new DomainCSVAggregatorStrategy(new TruncateDay()),
      new DomainCSVAggregatorStrategy(new TruncateMonth()),
      new FullDomainCSVAggregatorStrategy(new TruncateMonth()),
      new CodeCSVAggregatorStrategy(new TruncateMonth()),
      new CodeCSVAggregatorStrategy(new TruncateDay())
      // TODO maybe put DOIs back?
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

  // Preprocess all files.
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
      case "process": preprocess(workingDir); break;
      case "aggregate": aggregate(workingDir); break;
      case "analyze": analyze(workingDir); break;
    }
  }
}