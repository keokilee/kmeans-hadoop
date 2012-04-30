package edu.hawaii.ics621.algorithms;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import edu.hawaii.ics621.DoublePoint;

public abstract class AbstractClusteringAlgorithm {
  protected static final int MAX_ITERATIONS = 1000;
  protected static final double EPSILON = 1E-8;
  
  public List<DoublePoint> cluster(List<DoublePoint> inputs, int numClusters) {
    List<DoublePoint> clusters = this.initialClusters(inputs, numClusters);
    // Use these clusters to cluster the inputs.
    return this.clusterInputs(inputs, clusters);
  }
  
  private List<DoublePoint> initialClusters(List<DoublePoint> inputs, int clusters) {
    List<DoublePoint> initClusters = new ArrayList<DoublePoint>();
    
    // In this simple implementation, we'll just pick random inputs for our initial clusters.
    Random generator = new Random();
    for (int i = 0; i < clusters; i++) {
      DoublePoint item = inputs.get(generator.nextInt(inputs.size()));
      while (initClusters.contains(item)) {
        item = inputs.get(generator.nextInt(inputs.size()));
      }
      
      initClusters.add(item);
    }
    
    return initClusters;
  }
  
  protected abstract List<DoublePoint> clusterInputs(List<DoublePoint> inputs, List<DoublePoint> clusters);
}
