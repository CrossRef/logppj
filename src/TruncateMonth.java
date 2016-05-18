package logpp;

public class TruncateMonth implements DateProjector {
  // Truncate a date string in the format "YYYY-MM-DD" to the first day of the month.
  public String project(String date) {
    return date.substring(0, 7) + "-01";
  }

  public String getName() {
    return "month";
  }
}