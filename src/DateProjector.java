package logpp;

// Project a date to another date. Both a strategy and a const.
public interface DateProjector {
  // Truncate a date string in the format "YYYY-MM-DD" to another noe.
  public String project(String date);

  // Name of this kind of projection, used in filenames.
  public String getName();
}