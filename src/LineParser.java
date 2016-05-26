package logpp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;

// Parse a line into the useful strings that we want, an array of [date doi referrer].
// Can deal with a number of different line formats.
// Stateful because it decides the most appropriate format. 
// re2 is too greedy, so don't cycline round. TODO Explain
public class LineParser {
  // Nearly all log lines match chis.
  // 101.226.33.216 HTTP:HDL "2016-01-05 03:55:23.962-0500" 1 1 23ms 10.1002/aic.14628 "200:0.na/10.1002" "http://www.sogou.com/"
  // 68.180.228.178 HTTP:HDL "2015-01-01 03:10:43.247Z" 1 1 195ms 10.1080/02732173.2012.628560 "200:0.na/10.1080" ""
  private Pattern re1 = Pattern.compile("^[^ ]+ [^ ]+ \"([^\"]+)\" [^ ]* [^ ]* [^ ]* ([^ ]*) \"[^\"]*\" \"([^\"]*)\"$");
  
  // MEDRA seem to have their own format altogether.
  // 130.186.99.39 HTTP:HDL "2015-12-01 00:01:47.639+0100" 1 1 350ms 10.1016/j.anbehav.2009.06.021
  private Pattern re2 = Pattern.compile("^[\\d.]+ [^ ]+ \"([^\"]+)\" \\d+ \\d+ [^ ]+ (.+)$");

  private Pattern[] patterns = new Pattern[] { re1, re2 };

  // The currently selected pattern.
  private Pattern pattern = null;

  // Parse string into [dateString, DOI, referrer]
  public String[] parse(String line) {
    // First time, find the one that works. This will be reset every new file.
    if (this.pattern == null) {
      // Try all the patterns to see which one works.
      for (Pattern tryPattern : this.patterns) {
        String[] result = this.parseWith(line, tryPattern);

        // System.out.println("Try pattern " + tryPattern.toString());
        
        if (result != null) {
          this.pattern = tryPattern;
          // System.out.println("Worked!" + Arrays.toString(result));
          return result;
        }
      }      
    } else { 
      // Could be null.      
      return this.parseWith(line, this.pattern);
    }

    return null;
  }

  private String[] parseWith(String line, Pattern pattern) {
    Matcher matcher = pattern.matcher(line);

    if (!matcher.matches()) {
      return null;
    }

    // Lines conforming to re2 simply don't include the referrer.
    if (pattern == this.re1) {
      String date = matcher.group(1);
      String doi = matcher.group(2).toLowerCase();
      String referrer = matcher.group(3);
      return new String[] {date, doi, referrer};
    } else if (pattern == this.re2) {
      String date = matcher.group(1);
      String doi = matcher.group(2).toLowerCase();
      String referrer = "unknown.special";
      return new String[] {date, doi, referrer};
    } 

    return null;
  }
}