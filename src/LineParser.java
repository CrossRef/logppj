package logpp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;

// Parse a line into the useful strings that we want, an array of [date doi referrer].
// Can deal with a number of different line formats.
// Stateful because it caches the most recent working parser.
class LineParser {
  // Nearly all log lines match chis.
  // 101.226.33.216 HTTP:HDL "2016-01-05 03:55:23.962-0500" 1 1 23ms 10.1002/aic.14628 "200:0.na/10.1002" "http://www.sogou.com/"
  private Pattern re1 = Pattern.compile("^[\\d.]+ [a-zA-Z:]+ \"([0-9Z:\\-+ .]+)\" \\d+ \\d+ [a-zA-Z0-9]+ ([^ ]+) \"[^\"]+\" \"([^\"]*)\"$");
  
  // Slight variation on re1, no quotes round the DOI.
  // 208.115.113.93 HTTP:HDL "2015-12-01 00:00:01.149Z" 1 1 114ms 10.1016/j.bmcl.2006.12.066 "200:0.na/10.1016" "http://xyz.com"
  private Pattern re2 = Pattern.compile("^[a-f0-9.:]+ [a-zA-Z:]+ \"([0-9Z:\\-+ .]+)\" \\d+ \\d+ [a-zA-Z0-9]+ ([^ ]+) [^ ]+ \"([^\"]*)\"$");

  // MEDRA seem to have their own format altogether.
  // 130.186.99.39 HTTP:HDL "2015-12-01 00:01:47.639+0100" 1 1 350ms 10.1016/j.anbehav.2009.06.021
  private Pattern re3 = Pattern.compile("^[\\d.]+ [a-zA-Z:]+ \"([0-9:\\-+ .Z]+)\" \\d+ \\d+ [a-zA-Z0-9]+ (.+)$");

  private Pattern[] patterns = new Pattern[] { re1, re2, re3 };

  // The current pattern (one that worked last).
  private Pattern pattern = re1;

  // Parse string into [dateString, DOI, referrer domain]
  String[] parse(String line) {
    // Try the last one that worked.
    String[] result = this.parseWith(line, this.pattern);
    
    if (result != null) {
      return result;
    }

    // Try all the patterns to see which one works.
    for (Pattern tryPattern : this.patterns) {
      result = this.parseWith(line, tryPattern);

      // System.out.println("Try pattern " + tryPattern.toString());
      
      if (result != null) {
        this.pattern = tryPattern;
        // System.out.println("Worked!" + Arrays.toString(result));
        return result;
      }
    }

    // System.out.println("Failed!");
    return null;
  }

  String[] parseWith(String line, Pattern pattern) {
    Matcher matcher = pattern.matcher(line);

    if (!matcher.matches()) {
      return null;
    }

    if (pattern == this.re1 || pattern == this.re2) {
      String date = matcher.group(1);
      String doi = matcher.group(2);
      String referrer = matcher.group(3);
      return new String[] {date, doi, referrer};
    } else if (pattern == this.re3) {
      String date = matcher.group(1);
      String doi = matcher.group(2);
      String referrer = "unknown.special";
      return new String[] {date, doi, referrer};
    } 

    return null;
  }
}