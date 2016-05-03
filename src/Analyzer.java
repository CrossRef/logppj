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


public class Analyzer {
  File inputDirectory;
  File outputDirectory;

  public Analyzer(File inputDirectory, File outputDirectory) {
    this.inputDirectory = inputDirectory;
    this.outputDirectory = outputDirectory;
  }
  
  public void run(AnalyzerStrategy strategy) throws FileNotFoundException, UnsupportedEncodingException, IOException {
    long totalLines = 0;
    Writer output = new BufferedWriter(new FileWriter(new File(this.outputDirectory, strategy.fileName())));
    strategy.assignOutputFile(output);

    for (int partitionNumber = 0; partitionNumber < strategy.getNumPartitions(); partitionNumber++) {
      strategy.enterPartition(partitionNumber);

      // Take all files in the input directory and offer to the analyzer.
      for (File inputFile : this.inputDirectory.listFiles()) {
        String filename = inputFile.getName();

        if (!filename.matches(strategy.getInputFileRegex())) {
          continue;
        }

        BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));

        String lineInput;
        while ((lineInput = input.readLine()) != null) {
          totalLines++;

          strategy.feed(lineInput);
        }

        input.close();
      }
    }

    strategy.finish();
    output.close();
  }
}
