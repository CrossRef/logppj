package logpp;

// A partition space and the means by which to assign objects to a partition.
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
}
