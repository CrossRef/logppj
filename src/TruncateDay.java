package logpp;

public class TruncateDay implements DateProjector {
  public String project(String date) {
    // Don't need to do anything.
    return date;
  }

  public String getName() {
    return "day";
  }
}