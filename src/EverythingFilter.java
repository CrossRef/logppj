package logpp;

public class EverythingFilter implements Filter {
 public boolean keep(String domain) {
    return true;
  }

  public String getName() {
    return "unfiltered";
  }
}