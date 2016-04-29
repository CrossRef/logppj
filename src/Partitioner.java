package logpp;

public class Partitioner {
  int numPartitions;

  public Partitioner(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public int partition(Object obj) {
    int hashCode = obj.hashCode();
    if (hashCode < 0) {
      hashCode *= -1;
    }
    return hashCode % numPartitions;
  }

  public int partition(Object a, Object b) {
    int hashCode = a.hashCode() + b.hashCode();
    if (hashCode < 0) {
      hashCode *= -1;
    }
    return hashCode % numPartitions;
  }

  public int partition(Object a, Object b, Object c) {
    int hashCode = a.hashCode() + b.hashCode() + c.hashCode();
    if (hashCode < 0) {
      hashCode *= -1;
    }
    return hashCode % numPartitions;
  }

}
