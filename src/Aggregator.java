package logpp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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


public class Aggregator {
  File inputDirectory;
  File outputDirectory;

  public Aggregator(File inputDirectory, File outputDirectory) {
    this.inputDirectory = inputDirectory;
    this.outputDirectory = outputDirectory;
  }
  
  public void run(AggregatorStrategy strategy) throws FileNotFoundException, UnsupportedEncodingException, IOException {
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

      Writer output = new BufferedWriter(new FileWriter(new File(this.outputDirectory, strategy.fileName(filename))));
      long totalLines = 0;

      // Split the file into partitions.
      for (int partitionNumber = 0; partitionNumber < numPartitions; partitionNumber++) {
        System.out.format("%s: Partition %d / %d\n", filename, partitionNumber, numPartitions-1);

        // New handle each time. Doesn't happen often.
        BufferedReader input = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inputFile)), "UTF-8"));

        String lineInput;
        while ((lineInput = input.readLine()) != null) {
          // Line is [date, doi, code, domain].
          String[] line = lineInput.split("\t");

          // In transition between new and old format, some unknown domains are represented as empty strings.
          // TODO remove when all logs re-parsed.
          if (line.length != 4) {
            line = new String[] {line[0], line[1], "U", "unknown.special"};
          }

          // Reject except for this partition.
          // Strategy knows how to partition.
          if (strategy.partition(line) != partitionNumber) {
            continue;
          }

          totalLines++;

          strategy.feed(line);

          if (totalLines % 1000000 == 0) {
            System.out.format("Processed lines: %d\n", totalLines);
            
            // This solves weird flushing issues.
            System.out.println("");
          }
        }
        
        System.out.format("Processed lines: %d\n", totalLines);

        // We've finished this partition for this month, flush out the counts and start again.
        strategy.write(output);
        strategy.reset();

        // So we can snoop on what's being written.
        output.flush();

        // Re-open input and seek to start for each partition pass.
        input.close();
      }

      output.close();
    }
  }
}