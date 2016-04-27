package logpp;

class Partitioner {
  int numPartitions;

  Partitioner(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  int partition(Object obj) {
    return obj.hashCode() % numPartitions;
  }
}
