package logpp;

import java.io.File;
import logpp.aggregatorstrategies.*;
import logpp.analyzerstrategies.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import java.util.Arrays;

public class Main {

  // Run all the analyses.
  static void analyze(String workingDir) throws FileNotFoundException, IOException  {
    String inputPath = workingDir + "/aggregated";
    String outputPath = workingDir + "/analyzed";


    // A domain filter to exclude ignored domains. Is able to cope if the files don't exist.
    Filter domainFilter = new FileDomainFilter(
      // Some defaults.
      new String[] {
        "doi.org",
        "crossref.org",
        "unknown.special"},

      // Files that contain filtered out domain names. May or may not exist.
      new String[] {
        workingDir + "/filter-domain-names.txt",
        workingDir + "/filter-full-domain-names.txt"
      });

    File input = new File(inputPath);
    File output = new File(outputPath);

    System.out.format("Process %s to %s\n", inputPath, outputPath);
    Analyzer analyzer = new Analyzer(input, output);

    AnalyzerStrategy[] strategies = new AnalyzerStrategy[] {
      new CodeTableAnalyzerStrategy(new TruncateDay()),
      new CodeTableAnalyzerStrategy(new TruncateMonth()),

      // Domain and full domain filtered and unfiltered.
      new FullDomainAnalyzerStrategy(new TruncateMonth(), new EverythingFilter()),
      new DomainAnalyzerStrategy(new TruncateDay(), new EverythingFilter()),
      new FullDomainAnalyzerStrategy(new TruncateMonth(), domainFilter),
      new DomainAnalyzerStrategy(new TruncateDay(), domainFilter),

      new GroupedFullDomainsAnalyzerStrategy(new EverythingFilter()),
      new GroupedFullDomainsAnalyzerStrategy(domainFilter),
      new FullDomainDomainAnalyzerStrategy(),
      
      // Top N once with unfiltered domains.
      new TopNDomainsTableAnalyzerStrategy(10, new TruncateDay(), new EverythingFilter()),
      new TopNDomainsTableAnalyzerStrategy(10, new TruncateMonth(), new EverythingFilter()),
      new TopNDomainsTableAnalyzerStrategy(100, new TruncateMonth(), new EverythingFilter()),

      // And once with filtered domains. Even if the files aren't present, referrers like doi.org will be removed.
      new TopNDomainsTableAnalyzerStrategy(10, new TruncateDay(), domainFilter),
      new TopNDomainsTableAnalyzerStrategy(10, new TruncateMonth(), domainFilter),
      new TopNDomainsTableAnalyzerStrategy(100, new TruncateMonth(), domainFilter)
    };

    try {
      for (AnalyzerStrategy strategy: strategies) {
        System.out.format("Analyze with strategy: %s \n", strategy.toString());
        analyzer.run(strategy);
        // As each strategy is stateful and stays in the GC graph, allow it to let go of its data (which can be sizable).
        strategy.dispose();
        System.out.format("Finished analyze with strategy: %s \n", strategy.toString());
      }
    } catch (Exception e) {
      System.err.println("Error:");
      e.printStackTrace();
    }
  }

  // Run all the distribution tasks.
  static void distribute(String workingDir) throws FileNotFoundException, IOException  {
    String inputPath = workingDir + "/analyzed";
    String outputPath = workingDir + "/distribute";

    System.out.format("Process %s to %s\n", inputPath, outputPath);

    DistributerStrategy[] strategies = new DistributerStrategy[] {
      // For the chunk files, split by a hash.
      new ChunkHashDistributerStrategy(inputPath, outputPath, "day-filtered-domain.csv-chunks"),
      new ChunkHashDistributerStrategy(inputPath, outputPath, "day-filtered-domain.csv-chunks"),
      new ChunkHashDistributerStrategy(inputPath, outputPath, "month-filtered-fulldomain.csv-chunks"),

      // And copy the same files anyway, someone might want them.
      new CopyDistributerStrategy(inputPath, outputPath, "day-filtered-domain.csv-chunks"),
      new CopyDistributerStrategy(inputPath, outputPath, "day-filtered-domain.csv-chunks"),
      new CopyDistributerStrategy(inputPath, outputPath, "month-filtered-fulldomain.csv-chunks"),

      new ChunkHashDistributerStrategy(inputPath, outputPath, "filtered-grouped-fulldomain.csv-chunks"),
      new ChunkHashDistributerStrategy(inputPath, outputPath, "fulldomain-domain.csv-chunks"),
      
      // CSV files are ready to use, just copy over.
      // We don't distribute 'all' domain files like `day-top-100-all-domains.csv`, only the filtered ones.
      new CopyDistributerStrategy(inputPath, outputPath, "month-code.csv"),
      new CopyDistributerStrategy(inputPath, outputPath, "day-code.csv"),
      new CopyDistributerStrategy(inputPath, outputPath, "day-top-10-filtered-domains.csv"),
      new CopyDistributerStrategy(inputPath, outputPath, "month-top-10-filtered-domains.csv"),
      new CopyDistributerStrategy(inputPath, outputPath, "month-top-100-filtered-domains.csv")
    };

    try {
      for (DistributerStrategy strategy: strategies) {
        System.out.format("Distribute with strategy: %s \n", strategy.toString());
        strategy.run();
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
    

    AggregatorStrategy[] strategies = new AggregatorStrategy[] {
      new DomainCountCSVAggregatorStrategy(new TruncateDay()),
      new DomainCountCSVAggregatorStrategy(new TruncateMonth()),
      new FullDomainCountCSVAggregatorStrategy(new TruncateMonth()),
      new CodeCountCSVAggregatorStrategy(new TruncateMonth()),
      new CodeCountCSVAggregatorStrategy(new TruncateDay()),

      new GroupedFullDomainsAggregatorStrategy()
      // TODO maybe put DOIs back?
    };

    
    try {
      List<Thread> threads = new ArrayList<>();
      for (AggregatorStrategy strategy: strategies) {
        System.out.format("Aggregate with strategy: %s \n", strategy.toString());
        Aggregator aggregator = new Aggregator(input, output, strategy);
        Thread thread = new Thread(aggregator);
        threads.add(thread);
        thread.start(); 
        System.out.format("Finished aggregate with strategy: %s \n", strategy.toString());
      }

      for (Thread thread: threads) {
        synchronized(thread) {
          System.out.println("Wait..." + thread.toString());
          thread.join();
          System.out.println("Done " + thread.toString());
        }
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

    ConcurrentParser parser = new ConcurrentParser(inputDir, outputDir);

    // This designed for supervised use. Don't try to recover.
    try {
      parser.run();
    } catch (Exception e) {
      System.err.println("Error:");
      e.printStackTrace();
    }
  }

  public static void main(String[] argv) throws FileNotFoundException, IOException {
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
      case "distribute": distribute(workingDir); break;
    }
  }
}