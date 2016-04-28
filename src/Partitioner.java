package logpp;

class Partitioner {
  int numPartitions;

  Partitioner(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  int partition(Object obj) {
    int hashCode = obj.hashCode();
    if (hashCode < 0) {
      hashCode *= -1;
    }
    return hashCode % numPartitions;
  }
}
