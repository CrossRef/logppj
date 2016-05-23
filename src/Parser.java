package logpp;

import logpp.etld.ETLD;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.zip.GZIPOutputStream;
import java.io.BufferedWriter;
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
import java.util.Arrays;

// A parser for log files.
// Stateful, because it holds several file handles for multiplexing output.
public  class Parser {
  // A parser for effective TLDs.
  ETLD etld;

  // To indicate that it wasn't supplied.
  static String UNKNOWN_DOMAIN = "unknown.special";

  // To indicate that it was a local file.
  static String LOCAL_DOMAIN = "local.special";

  // Various codes.
  static String CODE_HTTP = "H"; // for HTTP protocol
  static String CODE_HTTPS = "S"; // for HTTPS protocol
  static String CODE_FTP = "F"; // for FTP protocol.
  static String CODE_FILE = "L"; // for file:// protocol.
  static String CODE_UNKNOWN = "U"; // for unknown protocol, but domain supplied
  static String CODE_NO_INFO = "N"; // for no information.
  static String CODE_WEIRD = "W"; // for weird (e.g. readcube)

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

  // Mapping if possible prefixes to full string replacement.
  // NB matches will be post-lowercase.
  private String[][] PREFIX_PAIRS = new String[][] {
    // Found with with capital 'R' and 'C' in the wild.
    new String[] {"app:/readcube.swf", "weird://readcube.special"},

    // Don't know where this comes from. Looks like generic SWF file.
    // https://en.wikipedia.org/wiki/Adobe_SWC_file
    new String[] {"app:/library.swf", "weird://unknown.special"},

    // Page number? 
    // E.g."app:/p822.swf"
    new String[] {"app:/p", "weird://unknown.special"},

  };

  public Parser(File inputDirectory, File outputDirectory) {
    this.etld = new ETLD();
    // Add the special "special" special domain for recording special things like UNKNOWN_DOMAIN.
    this.etld.addLine("special");
    this.inputDirectory = inputDirectory;
    this.outputDirectory = outputDirectory;
  }

  // Parse referrer string into [code, whole-domain, subdomains, domain, path]
  // See consts above for code definitions.
  // Always return, sometimes empty string for domain.
  private String[] parseReferrer(String referrer) {
    String code = CODE_NO_INFO;
    
    // If there isn't one, use a placeholder.
    String host = UNKNOWN_DOMAIN;

    // Path mostly empty.
    String path = "";

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

        // Weird prefixes crop up annoyingly often. Especially feedspot.
        // e.g. "Feedspotbot: http://www.feedspot.com"
        if (referrer.matches("^.*?https?://.*$")) {
          referrer = referrer.replaceAll("^.*?(https?://.*)$", "$1");
        }

        // Catch these mappings before we build the host.
        for (String[] prefixPair : PREFIX_PAIRS) {
          if (referrer.startsWith(prefixPair[0])) {
            referrer = prefixPair[1];
          }
        }

        // Next most common is a well-formed URL.
        URL url = new URL(referrer);
        host = url.getHost();
        String protocol = url.getProtocol();
        path = url.getPath();

        // In order of likelihood. All of these have been observed!
        // https://docs.oracle.com/javase/8/docs/technotes/guides/language/strings-switch.html
        switch (protocol) {
          case "http":
            code = CODE_HTTP;
            break;
          case "https":
            code = CODE_HTTPS;
            break;
          case "ftp":
            // Yep.
            code = CODE_FTP;
            break;
          case "file":
            code = CODE_FILE;
            host = LOCAL_DOMAIN;
            break;
          case "weird":
            // Can be introduced by PREFIX_PAIRS.
            code = CODE_WEIRD;
            break;
          default:
            // We got a valid URL but don't know what the protocol is.
            // Not safe to supply the host because we don't know it's meaningful.
            System.out.println("Unrecognised referrer protocol: " + url.getProtocol());
            System.out.println(referrer);
            code = CODE_UNKNOWN;
            host = UNKNOWN_DOMAIN;
        }
      } catch (MalformedURLException exception) {
        // It's not a valid URL but it could be a valid domain without a scheme, e.g. "ask.com". In this case, try to build a URL with it.
        // But first some well known exceptions (which might otherwise squash into a URL if we put "http://" in front).
        if (referrer.equals("about:blank")) {
            code = CODE_NO_INFO;
            host = LOCAL_DOMAIN;
          } else if (referrer.matches("^[A-Z]:\\\\.*$")) {
            // Some Windows drive. 
            code = CODE_FILE; 
            host = LOCAL_DOMAIN;
          } else if (referrer.startsWith("mhtml:")) {
            // MIME HTML, Internet Explorer saved page. Count as local file.
            code = CODE_FILE;
            host = LOCAL_DOMAIN;
          } else if (referrer.contains("dlvr.it")) {
            // e.g. "dlvrId=bcf15c60aa2d9e4f737603e82da64730; expires=Sat, 16-Feb-2013 21:24:04 GMT; path=/; domain=dlvr.it"
            code = CODE_UNKNOWN;
            host = "dlvr.it";
          } else if (referrer.contains("bit.ly")) {
            // e.g. "_bit=50f10b43-00008-07b21-401cf10a;domain=.bit.ly;expires=Thu Jul 11 07:05:39 2013;path=/; HttpOnly"
            code = CODE_UNKNOWN;
            host = "bit.ly";
          } else if (referrer.startsWith("javascript:")) {
            // e.g. "javascript:expandCollapse('infoBlockID', true);"
            code = CODE_UNKNOWN;
            host = UNKNOWN_DOMAIN;
          } else {
            try {
              // If it sticking http in front of it makes it a workable URL, use that.
              URL url = new URL("http://" + referrer);
              code = CODE_HTTP;
              host = url.getHost();
              path = url.getPath();
            } catch (MalformedURLException innerException) {
              // If we're here there's not much hope!
              System.out.println("ERROR " + innerException.toString());
              System.out.println(referrer);
          }
        }
      }
    }

    // Finally.
    // Referrers like: "http:///reload/10.1007/BF02821190" and "http:///" constitute valid java.net.URLs,
    // giving empty domains. Ensure that under no circumstances we return an empty string.
    if (host.length() == 0) {
      host = UNKNOWN_DOMAIN;
      code = CODE_UNKNOWN;
    }

    String[] domainParts = this.etld.getParts(host);

    return new String[] {code, host, domainParts[0], domainParts[1], path};
  }

  // Return the file handle.
  // Create the file if necessary, but don't over-write.
  private Writer getOutputFile(String yearMonth) throws IOException {
    Writer writer = this.monthFiles.get(yearMonth);
    if (writer != null) {
      return writer;
    } else {
      File f = new File(this.outputDirectory, yearMonth + ".gz");

      if (f.exists()) {
        System.err.format("WARNING: Not overwriting file: %s\n", f.getPath());
        writer = nullWriter;
      } else {
        OutputStream fileStream = new FileOutputStream(f);
        OutputStream gzipStream = new GZIPOutputStream(fileStream);
        Writer encoder = new OutputStreamWriter(gzipStream, "UTF-8");
        writer = new BufferedWriter(encoder);
      }

      this.monthFiles.put(yearMonth, writer);

      return writer;
    }
  }

  public void run() throws FileNotFoundException, IOException, UnsupportedEncodingException, ParseException {
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

    // Number of lines where input was malformatted, skipping.
    int malformedSkips = 0;

    // Keep track of the most current year month string. Not monotonic, could jump anywhere.
    String previousYearMonth  = "";
    // The current output file, cached between lines. Only changes when the month changes, which is virtually never (well, about 1 time in a million)
    Writer outputFile = null;

    for (File inputFile : this.inputDirectory.listFiles()) {
      // LineParser uses the first format that works for this file.
      // So a new one each file.
      LineParser lineParser = new LineParser();

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
        try {

          // Newlines count for something.
          totalChars += line.length() + 1;

          String[] match = lineParser.parse(line);

          // It's possible that lines fail to parse.
          //  - OpenURL lines are missing fields so skip them.
          //  - Some DOIs have spaces in them and it's impossible to parse.
          //  - Sometimes huge fragments of HTML are passed in.
          //  - Sometimes weird bytes creep in. Call it solar radiation.
          // The failure rate is roughly 0.05%
          if (match == null) {
            if (openurlRe.matcher(line).find()) {
              openUrlIgnores ++;
            } else {
              failedLines ++;
            }
          } else {
            String dateString = match[0];
            String doi = match[1];
            String referrerString = match[2];

            // Year-month for log file name, year-month-day for log entry.
            String[] parsedDate = this.dateParser.parseDate(dateString);
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

            // Get the full domain and code.
            String[] parsedReferrer = parseReferrer(referrerString);
            String referrerCode = parsedReferrer[0];
            String referrerFullDomain = parsedReferrer[1];
            String referrerSubdomains = parsedReferrer[2];
            String referrerDomain = parsedReferrer[3];
            String referrerPath = parsedReferrer[4];

            if (yearMonthDay.contains("\t") ||
                doi.contains("\t") ||
                referrerCode.contains("\t") ||
                referrerFullDomain.contains("\t") ||
                referrerSubdomains.contains("\t") ||
                referrerDomain.contains("\t") ||
                referrerPath.contains("\t")) {
              System.err.format("ERROR: Malformed input line: %s\n", line);
              malformedSkips ++;
            } else {
            outputFile.write(yearMonthDay);
            outputFile.write("\t");
            outputFile.write(doi);
            outputFile.write("\t");
            outputFile.write(referrerCode);
            outputFile.write("\t");
            outputFile.write(referrerFullDomain);
            outputFile.write("\t");
            outputFile.write(referrerSubdomains);
            outputFile.write("\t");
            outputFile.write(referrerDomain);
            outputFile.write("\t");
            outputFile.write(referrerPath);
            outputFile.write("\n");
          }

            
            
          }

          totalLines ++;

          // Every million lines. 
          if (totalLines % 1000000 == 0) {
            System.out.format("Processed lines: %d, characters: %d, failed: %d, OpenURL: %d, malformed skips: %d\n", totalLines, totalChars, failedLines, openUrlIgnores, malformedSkips);
            
            // This solves weird flushing issues.
            System.out.println("");
          }
        } catch (Exception e) {
          System.out.println("ERROR ");
          e.printStackTrace();
          System.out.println(line);
        }
      }

      bufferedReader.close();

      totalFiles++;
      System.out.format("Finished file! Processed lines: %d, characters: %d, failed: %d, OpenURL: %d, malformed skips: %d\n", totalLines, totalChars, failedLines, openUrlIgnores, malformedSkips);
    }

    for (Writer w: this.monthFiles.values()) {
      w.close();
    }

    System.out.format("Finished all files! Processed lines: %d, characters: %d, failed: %d, OpenURL: %d\n", totalLines, totalChars, failedLines, openUrlIgnores);
    System.out.println("");
  }
}