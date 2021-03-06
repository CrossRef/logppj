package logpp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.io.FileInputStream;
import java.util.concurrent.atomic.AtomicInteger;

// Take all files from the `processed` directory and perform various aggregations, as determined by an `AggregatorStrategy`.
public class Aggregator implements Runnable {
  File inputDirectory;
  File outputDirectory;
  AggregatorStrategy strategy;

  public Aggregator(File inputDirectory, File outputDirectory, AggregatorStrategy strategy) {
    this.inputDirectory = inputDirectory;
    this.outputDirectory = outputDirectory;
    this.strategy = strategy;

    if (!this.inputDirectory.exists()) {
      throw new IllegalArgumentException(String.format("Error: Input directory %s doesn't exist\n", this.inputDirectory));
    }

    if (!this.outputDirectory.exists()) {
      throw new IllegalArgumentException(String.format("Error: Output directory %s doesn't exist\n", this.outputDirectory));
    }
  }
  
  public void run() {
    try {
    final AtomicInteger totalLinesCounter = new AtomicInteger(0);

    Thread reporter = new Thread() {
        public void run() {
          try {
            // Interrupt breaks the thread.
            while (! Thread.currentThread().isInterrupted()) {
              System.out.format("Tick '" + Aggregator.this.strategy.toString() + "'. Processed lines: %d\n", totalLinesCounter.get());
              Thread.sleep(10000);
            }
          } catch (InterruptedException e) {}
        
      }
    };

      reporter.start();

      int numPartitions = strategy.numPartitions();
      
      // One file is a month, so counts are self-contained.
      // One input file is exactly a month and corresponds to exactly one output file.
      for (File inputFile : this.inputDirectory.listFiles()) {
        // Filename will be the YYYY-MM.gz .
        String filename = inputFile.getName();
        // Ignore .DS_Store and friends.
        if (!filename.matches("\\d\\d\\d\\d-\\d\\d\\.gz")) {
          continue;
        }

        // Drop the extension.
        filename = filename.substring(0, 7);

        // If this aggregation already ran skip it.
        File outputFile = new File(this.outputDirectory, strategy.fileName(filename));
        if (outputFile.exists()) {
          System.out.format("Aggregate output file %s already exists, skipping.\n", outputFile.toString());
          continue;
        }

        Writer output = new BufferedWriter(new FileWriter(outputFile));
        long totalLines = 0;

        // Split the file into partitions.
        for (int partitionNumber = 0; partitionNumber < numPartitions; partitionNumber++) {
          System.out.format("%s: Partition %d / %d\n", filename, partitionNumber, numPartitions-1);

          // New handle each time. Doesn't happen often.
          BufferedReader input = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inputFile), 131072), "UTF-8"), 131072);

          String lineInput;
          while ((lineInput = input.readLine()) != null) {
            // Line is [date, doi, code, full-domain, subdomains, domain, path].
            String[] line = lineInput.split("\t", -1);

            if (line.length != 7) {
              System.err.format("Error: Ignoring line with %d parts: %s\n", line.length, lineInput);
              continue;
            }

            // Reject except for this partition.
            // Strategy knows how to partition.
            if (strategy.partition(line) != partitionNumber) {
              continue;
            }

            totalLines++;
            totalLinesCounter.getAndIncrement();

            strategy.feed(line);

            if (totalLines % 1000000 == 0) {
              System.out.format("Processed lines: %d\n", totalLines);
              
              // This solves weird flushing issues.
              System.out.println("");
            }
          }
          
          // We've finished this partition for this month, flush out the counts and start again.
          strategy.write(output);
          strategy.reset();

          // So we can snoop on what's being written.
          output.flush();

          // Re-open input and seek to start for each partition pass.
          input.close();
        }

        output.close();
        reporter.interrupt();
      }
      
    } catch (Exception e) {
      // If there's an exception, blow the whole thing up. This tool is meant for supervised use.
      System.err.println("ERROR: " + e.toString());
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("Aanalyzer finished.");
  }
}