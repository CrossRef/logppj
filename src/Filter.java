package logpp;

public interface Filter {
 public boolean keep(String domain);
 public String getName();
}