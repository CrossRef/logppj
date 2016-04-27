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

// An output stream that does nothing.
// Used as a marker for 'don't write anything' so isn't ever actually written to anyway.
class NullOutputStream extends Writer {
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

// A date parser. Stateful, as it uses the appropriate format and remembers what it used last time.
class DateParser {
  // Different standards in use at different times. We have a pallate of formats available to us.

  // This one, as used in 2013 files, is compared by reference.
  private DateTimeFormatter twentyThirteenFormatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy");

  private DateTimeFormatter[] dateFormats = new DateTimeFormatter[] {
    twentyThirteenFormatter,

    // Used in e.g. 2016 opoce files.
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSz"),

    // Used in e.g. 2016 sps2 files
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ"),

    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS'Z'")
  };

  // The current datetimeformatter that we've found that works. Can swap around depending on log files.
  private DateTimeFormatter currentDateFormatter = this.twentyThirteenFormatter;

  // Parse date into array of ["YYYY-MM", YYYY-MM-DD"] in UTC.
  String[] parseDate(String dateStr) {
    // First try the current one, assuming it will work.
    try {
      return this.parseDateWithFormatter(dateStr, this.currentDateFormatter);
    } catch (DateTimeParseException e) {
      // If that didn't work, try the others.

      for (DateTimeFormatter formatter : this.dateFormats) {
        try {
          String[] result = this.parseDateWithFormatter(dateStr, formatter);

          // If that worked without throwing an exception, save this one and return.
          this.currentDateFormatter = formatter;

          return result;
        } catch (DateTimeParseException ee) {
          // If that didn't work, no bother, try the next.
        }
      }

      // Now we're out of options!
      System.out.println("Couldn't parse date! " + dateStr);
      System.exit(1);
      return null;
    }
  }

  String[] parseDateWithFormatter(String dateStr, DateTimeFormatter formatter) {
    // System.out.format("Try \"%s\" with %s\n", dateStr, formatter.toString());

    // 2013 formatter has non-standard timezones. Turn these into well-understood offsets.
    if (formatter == this.twentyThirteenFormatter) {
      dateStr = dateStr.replace("EST", "-05:00");
      dateStr = dateStr.replace("EDT", "-04:00");
    }

    // Translate time into UTC.
    ZonedDateTime offsetDateTime = ZonedDateTime.parse(dateStr, formatter).toInstant().atZone(ZoneOffset.UTC);

    String yearMonthDay = String.format("%04d-%02d-%02d", offsetDateTime.getYear(), offsetDateTime.getMonth().getValue(), offsetDateTime.getDayOfMonth());
    String yearMonth = String.format("%04d-%02d", offsetDateTime.getYear(), offsetDateTime.getMonth().getValue());

    return new String[] {yearMonth, yearMonthDay};
  }

}

// A parser for log files.
// Stateful, because it holds several file handles for multiplexing output.
class Parser {
  private Pattern re = Pattern.compile("^([\\d.]+) ([a-zA-Z:]+) (\"[^\"]*\") (\\d+) (\\d+) ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$");

  // Pattern to handle OpenURL requests (for error handling).
  private Pattern openurlRe = Pattern.compile("^([\\d.]+) HTTP:OpenURL");

  // Mapping of date as YYYY-MM to file handle, which could be a NullOutputStream.
  private Map<String, Writer> monthFiles = new HashMap<String, Writer>();

  private DateParser dateParser = new DateParser();

  private File inputDirectory;
  private File outputDirectory;

  // A singleton null writer.
  // This is used when a file isn't being overwritten. 
  // Reference comparison against this.
  private Writer nullWriter = new NullOutputStream();

  Parser(File inputDirectory, File outputDirectory) {
    this.inputDirectory = inputDirectory;
    this.outputDirectory = outputDirectory;
  }

  // Parse referrer string into [code domain]
  // Code is 
  // "H" for HTTP protocol
  // "S" for HTTPS protocol
  // "F" for FTP protocol.
  // "L" for file protocol.
  // "U" for unknown protocol, but domain supplied
  // "N" for no information.
  // "W" for weird (e.g. readcube)
  // Always return, sometimes empty string for domain.
  String[] parseReferrer(String referrer) {
    String code = "N";
    String host = "";

    // Most common is empty string.
    if (referrer.length() > 1) {
      try {
        // First some preprocessing heuristics which have to be done first.

        // e.g. "Referer: http://www.zetesis.it/people/parisia/".
        // (Official spelling of referrer is wrong)
        if (referrer.startsWith("Referer:")) {
          referrer = referrer.replaceAll("Referer: *", "");
        } else if (referrer.startsWith("REFERER:")) {
          // e.g. "REFERER: http://archaeology.about.com/gi/o.htm"
          referrer = referrer.replaceAll("REFERER: *", "");
        }

        // Next most common is a well-formed URL.
        URL url = new URL(referrer);
        host = url.getHost();
        String protocol = url.getProtocol();

        // In order of likelihood. All of these have been observed!
        // https://docs.oracle.com/javase/8/docs/technotes/guides/language/strings-switch.html
        switch (protocol) {
          case "http":
            code = "H";
            break;
          case "https":
            code = "S";
            break;
          case "ftp":
            // Yep.
            code = "F";
            break;
          case "file":
            code = "L";
            host = "file.special";
            break;
          default:
            // We got a valid URL but don't know what the protocol is.
            // Not safe to supply the host because we don't know it's meaningful.
            System.out.println("Unrecognised referrer protocol: " + url.getProtocol());
            System.out.println(referrer);
            code = "O";
            host = "";
        }
      } catch (MalformedURLException exception) {
        // It's not a valid URL but it could be a valid domain without a scheme, e.g. "ask.com". In this case, try to build a URL with it.
        // But first some well known exceptions (which might otherwise squash into a URL if we put "http://" in front).
        if (referrer.equals("about:blank")) {
            code = "N";
            host = "";
          } else if (referrer.startsWith("app:/ReadCube.swf")) {
            code = "W";
            host = "readcube.special";
          } else if (referrer.matches("^[A-Z]:\\\\.*$")) {
            // Some Windows drive. 
            code = "L"; 
          } else if (referrer.startsWith("mhtml:")) {
            // MIME HTML, Internet Explorer saved page. Count as local file.
            code = "L";
            host = "file.special";
          } else if (referrer.contains("domain=dlvr.it")) {
            // e.g. "dlvrId=bcf15c60aa2d9e4f737603e82da64730; expires=Sat, 16-Feb-2013 21:24:04 GMT; path=/; domain=dlvr.it"
            code = "U";
            host = "dlvr.id";
          } else if (referrer.contains("domain=.bit.ly")) {
            // e.g. "_bit=50f10b43-00008-07b21-401cf10a;domain=.bit.ly;expires=Thu Jul 11 07:05:39 2013;path=/; HttpOnly"
            code = "U";
            host = "bit.ly";
          } else if (referrer.startsWith("javascript:")) {
            // e.g. "javascript:expandCollapse('infoBlockID', true);"
            code = "N";
            host = "";
          } else {
            try {
              // If it works, use that.
              URL url = new URL("http://" + referrer);
              code = "U";
              host = url.getHost();
            } catch (MalformedURLException innerException) {
              // If we're here there's not much hope!
              System.out.println("ERROR " + innerException.toString());
              System.out.println(referrer);
          }
        }
      }
    }

    return new String[] {code, host};
  }

  // Return the file handle.
  // Create the file if necessary, but don't over-write.
  Writer getOutputFile(String yearMonth) throws IOException {
    Writer writer = this.monthFiles.get(yearMonth);
    if (writer != null) {
      return writer;
    } else {
      File f = new File(this.outputDirectory, yearMonth);

      if (f.exists()) {
        System.err.format("WARNING: Not overwriting file: %s\n", f.getPath());
        writer = nullWriter;
      } else {
        writer = new BufferedWriter(new FileWriter(f));
      }

      this.monthFiles.put(yearMonth, writer);

      return writer;
    }
  }

  void run() throws FileNotFoundException, IOException, UnsupportedEncodingException, ParseException {
    System.out.println("Run");

    // Total successful lines.
    long totalLines = 0;

    // Total consumed chars.
    // Overflows int many times over!
    long totalChars = 0;

    // Total lines where parsing failed.
    long failedLines = 0;

    // Total lines ignored because it was openURL.
    long openUrlIgnores = 0;

    // Total files that were processed.
    int totalFiles = 0;

    // Keep track of the most current year month string. Not monotonic, could jump anywhere.
    String previousYearMonth  = "";
    // The current output file, cached between lines. Only changes when the month changes, which is virtually never (well, about 1 time in a million)
    Writer outputFile = null;

    for (File inputFile : this.inputDirectory.listFiles()) {
      // Ignore non-log files.
      if (!(inputFile.getName().startsWith("access_log") && inputFile.getName().endsWith(".gz"))) {
        continue;
      }

      System.out.format("Process input file %s, total %d\n", inputFile.getName(), totalFiles);

      InputStream fileStream = new FileInputStream(inputFile);
      InputStream gzipStream = new GZIPInputStream(fileStream);
      Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
      BufferedReader bufferedReader = new BufferedReader(decoder);

      String line;
      while ((line = bufferedReader.readLine()) != null) {
        // Newlines count for something.
        totalChars += line.length() + 1;

        Matcher matcher = re.matcher(line);
        
        // It's possible that lines fail to parse.
        //  - OpenURL lines are missing fields so skip them.
        //  - Some DOIs have spaces in them and it's impossible to parse.
        //  - Sometimes huge fragments of HTML are passed in.
        //  - Sometimes weird bytes creep in. Call it solar radiation.
        // The failure rate is roughly 0.05%
        if (!matcher.matches()) {
          if (openurlRe.matcher(line).find()) {
            openUrlIgnores ++;
          } else {
            failedLines ++;
          }
        } else {
          // We got a cleanly matched line.
          String dateStr = matcher.group(3);

          // Strip quotes.
          dateStr = dateStr.substring(1, dateStr.length() - 1);
          String[] parsedDate = this.dateParser.parseDate(dateStr);

          // Year-month for log file name, year-month-day for log entry.
          String yearMonth = parsedDate[0];
          String yearMonthDay = parsedDate[1];
          
          // Interrupt this processing to take a shortcut if necessary.
          // Decide on the output writer for this line. In nearly all cases it will be the same as last time.
          // If the year-month changed look up the new file.
          if (!yearMonth.equals(previousYearMonth)) {
            System.out.format("Change writer: %s -> %s\n", previousYearMonth, yearMonth);

            // Wight have a number of file handles on the go.
            // Save having too many fullish buffers hanging around.
            // First time round it's null.
            if (outputFile != null) {
              outputFile.flush();
            }

            outputFile = this.getOutputFile(yearMonth);
            previousYearMonth = yearMonth;
          }

          // If it is a file that we refused to over-write, don't bother parsing the rest.
          if (outputFile == this.nullWriter) {
            continue;
          }

          String doi = matcher.group(7);
          String status = matcher.group(8);
          String referrerStr = matcher.group(9);
          referrerStr = referrerStr.substring(1, referrerStr.length() - 1);

          String[] parsedReferrer = parseReferrer(referrerStr);
          String referrerCode = parsedReferrer[0];
          String referrerDomain = parsedReferrer[1];

          // «YYYY-MM-DD in UTC» «DOI» «code» «referring domain»
          outputFile.write(yearMonthDay);
          outputFile.write("\t");
          outputFile.write(doi);
          outputFile.write("\t");
          outputFile.write(referrerCode);
          outputFile.write("\t");
          outputFile.write(referrerDomain);
          outputFile.write("\n");
        }

        totalLines ++;

        // Every million lines. 
        if (totalLines % 1000000 == 0) {
          System.out.format("Processed lines: %d, characters: %d, failed: %d, OpenURL: %d", totalLines, totalChars, failedLines, openUrlIgnores);
          
          // This solves weird flushing issues.
          System.out.println("");
        }
      }

      bufferedReader.close();

      totalFiles++;
    }

    for (Writer w: this.monthFiles.values()) {
      w.close();
    }

    System.out.format("Finished! Processed lines: %d, characters: %d, failed: %d, OpenURL: %d", totalLines, totalChars, failedLines, openUrlIgnores);
    System.out.println("");
  }
}

// Map of strings to integers.
// Not threasafe but doesn't have to be.
class IdentityMap {
  Integer counter = 0;
  Map<String, Integer> entries = new HashMap<String, Integer>();
  Map<Integer, String> inverseEntries = new HashMap<Integer, String>();

  // Get mapping, creating ID if necessary.
  Integer get(String input) {
    Integer entry = this.entries.get(input);
    if (entry != null) {
      return entry;
    }

    counter++;
    
    this.entries.put(input, counter);
    this.inverseEntries.put(counter, input);

    // System.out.println("GET:" + input + " = " + counter);

    return counter;
  }

  // Get inverse mapping, returning null if not found.
  String getInverse(Integer input) {
    return this.inverseEntries.get(input);
  }

  Integer count() {
    return this.counter;
  }

  Integer size() {
    return this.entries.size();
  }

  Integer inverseSize() {
    return this.inverseEntries.size();
  }
}

class Partitioner {
  int numPartitions;

  Partitioner(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  int partition(Object obj) {
    return obj.hashCode() % numPartitions;
  }
}

// A particular kind of aggregation.
// This is a stateful strategy object with various callbacks. 
// Not threadsafe.
interface AggregatorStrategy {
  // How many partitions required.
  // Based on observed heuristics.
  int numPartitions();

  // Filename for this kind out output based on date.
  String fileName(String date);

  // Rather than have another layer of context objects and context object factories, allow reset of state between runs.
  void reset();

  // Which partition does this line belong to?
  int partition(String[] line);

  // Process the line.
  void feed(String[] line);

  // Write everything to the output file.
  void write(Writer writer) throws IOException;
}

// Count DOI per day.
class DOIAggregatorStrategy implements AggregatorStrategy {
  // Ignore counts under this value.
  static int CUTOFF = 10;
  long inputCount = 0;

  IdentityMap doiIds;

  // Map of DOI ID, Date string => count.
  HashMap<String, Integer> counter;
  Partitioner partitioner;

  DOIAggregatorStrategy() {
    this.reset();
  }

  public int numPartitions() {
    return 20;
  }

  public String fileName(String date) {
    return String.format("%s-doi",  date);
  }

  public void reset() {
    this.counter = new HashMap<String, Integer>();
    this.doiIds = new IdentityMap();
    this.partitioner = new Partitioner(this.numPartitions());
    this.inputCount = 0;
  }

  public int partition(String[] line) {
    return this.partitioner.partition(line[1]);
  }

  public void feed(String[] line) {
    Integer doiId = this.doiIds.get(line[1]);
    String key = line[0] + ":" + doiId;
    this.counter.put(key, this.counter.getOrDefault(key, 0) + 1);

    inputCount ++;
    if (inputCount % 1000000 == 0) {
      System.out.format("Processed %d lines. Frequency table size: %d. Identity map size %d (%d, %d) \n", this.inputCount, this.counter.size(), this.doiIds.count(), this.doiIds.size(), this.doiIds.inverseSize());
    }
  }

  public void write(Writer writer) throws IOException {
    for (Map.Entry<String, Integer> entry : this.counter.entrySet()) {
      Integer count = entry.getValue();
      if (count > CUTOFF) {
        String[] dateDoi = entry.getKey().split(":");
        String date = dateDoi[0];

        String doi = this.doiIds.getInverse(Integer.parseInt(dateDoi[1]));

        writer.write(date);
        writer.write("\t");

        writer.write(doi);
        writer.write("\t");

        writer.write(count.toString());
        writer.write("\n");
      }
    }
  }
}

class Aggregator {
  File inputDirectory;
  File outputDirectory;

  Aggregator(File inputDirectory, File outputDirectory) {
    this.inputDirectory = inputDirectory;
    this.outputDirectory = outputDirectory;
  }
  
  void run(AggregatorStrategy strategy) throws FileNotFoundException, UnsupportedEncodingException, IOException {
    int numPartitions = strategy.numPartitions();
    
    // One file is a month, so counts are self-contained.
    // One input file is exactly a month and corresponds to exactly one output file.
    for (File inputFile : this.inputDirectory.listFiles()) {
      // Filename will be the YYYY-MM.
      String filename = inputFile.getName();
      // Ignore .DS_Store and friends.
      if (!filename.matches("\\d\\d\\d\\d-\\d\\d")) {
        continue;
      }

      Writer output = new BufferedWriter(new FileWriter(new File(this.outputDirectory, strategy.fileName(filename))));
      long totalLines = 0;

      // Split the file into partitions.
      for (int partitionNumber = 0; partitionNumber < numPartitions; partitionNumber++) {
        System.out.format("%s: Partition %d / %d\n", filename, partitionNumber, numPartitions);

        // New handle each time. Doesn't happen often.
        BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"));

        String lineInput;
        while ((lineInput = input.readLine()) != null) {
          totalLines++;

          // Line is [date, doi, code, possibly-domain]. Might be 3 or 4 long.
          String[] line = lineInput.split("\t");

          // Reject except for this partition.
          // Strategy knows how to partition.
          if (strategy.partition(line) != partitionNumber) {
            continue;
          }

          strategy.feed(line);

          if (totalLines % 1000000 == 0) {
            System.out.format("Processed lines: %d", totalLines);
            
            // This solves weird flushing issues.
            System.out.println("");
          }
        }
        
        // We've finished this partition for this month, flush out the counts and start again.
        strategy.write(output);
        strategy.reset();

        // Re-open input and seek to start for each partition pass.
        input.close();
      }

      output.close();
    }
  }
}


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

    AggregatorStrategy doiStrategy = new DOIAggregatorStrategy();

    try {
      aggregator.run(doiStrategy);
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