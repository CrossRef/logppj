package logpp;

import java.time.format.DateTimeParseException;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


// A date parser. Stateful, as it uses the appropriate format and remembers what it used last time.
public class DateParser {
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

  // Compare this many characters to avoid re-parsing for lines EXCEPT twentyThirteenFormatter, as it's in a weird format.
  // "yyyy-MM-dd HH:mm" 
  private final int SIGNIFICANT_CHARS = 17;

  // The last date string truncated to SIGNIFICANT_CHARS.
  private String lastTruncatedDate = null;
  private String[] lastResult = null;

  // The current datetimeformatter that we've found that works. Can swap around depending on log files.
  private DateTimeFormatter currentDateFormatter = this.twentyThirteenFormatter;

  // Parse date into array of ["YYYY-MM", YYYY-MM-DD"] in UTC.
  public String[] parseDate(String dateStr) {
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

  private String[] parseDateWithFormatter(String dateStr, DateTimeFormatter formatter) {
    // System.out.format("Try \"%s\" with %s\n", dateStr, formatter.toString());

    // 2013 formatter has non-standard timezones. Turn these into well-understood offsets.
    if (formatter == this.twentyThirteenFormatter) {
      dateStr = dateStr.replace("EST", "-05:00");
      dateStr = dateStr.replace("EDT", "-04:00");
    }

    // Shortcut by comparing SIGNIFICANT_CHARS character of last date.
    // This results in a 6x speedup for 3,000,000 inputs (90 seconds to 15 seconds).
    // Doesn't work for twentyThirteenFormatter.
    if (formatter != this.twentyThirteenFormatter) {
      if (dateStr.substring(0, SIGNIFICANT_CHARS).equals(this.lastTruncatedDate)) {
        return this.lastResult;
      } else {
        // If we didn't match then there's a new date, so clear out last one.
        this.lastTruncatedDate = null;
        this.lastResult = null;
      }
    }

    // Translate time into UTC.
    ZonedDateTime offsetDateTime = ZonedDateTime.parse(dateStr, formatter).toInstant().atZone(ZoneOffset.UTC);

    String yearMonthDay = String.format("%04d-%02d-%02d", offsetDateTime.getYear(), offsetDateTime.getMonth().getValue(), offsetDateTime.getDayOfMonth());
    String yearMonth = String.format("%04d-%02d", offsetDateTime.getYear(), offsetDateTime.getMonth().getValue());

    this.lastResult = new String[] {yearMonth, yearMonthDay};
    this.lastTruncatedDate = dateStr.substring(0, SIGNIFICANT_CHARS);

    return this.lastResult;
  }

}
