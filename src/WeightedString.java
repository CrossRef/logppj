package logpp;

public class WeightedString implements Comparable<WeightedString> {
  private long weight;
  private String value;

  public WeightedString(String value, long weight) {
    this.weight = weight;
    this.value = value;
  }

  public int compareTo(WeightedString other) {
    return Long.compare(this.weight, other.getWeight());
  }

  public String getValue() {
    return this.value;
  }

  public long getWeight() {
    return this.weight;
  }

  public String toString() {
    return String.format("(%s, %d)", this.value, this.weight);
  }
}