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
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

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