package edu.hawaii.ics621.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import edu.hawaii.ics621.DoublePoint;

public class KMeans extends AbstractClusteringAlgorithm {

  @Override
  protected List<DoublePoint> clusterInputs(List<DoublePoint> inputs, List<DoublePoint> clusters) {
    Map<DoublePoint, List<DoublePoint>> results = new HashMap<DoublePoint, List<DoublePoint>>();
    List<DoublePoint> newClusters;
    DoublePoint tempPoint;
    boolean changed = true;
    int iterations = 0;
    
    while (changed && iterations < MAX_ITERATIONS) {
      changed = false;
      
      // Reset the table.
      results.clear();
      for (DoublePoint cluster : clusters) {
        results.put(cluster, new ArrayList<DoublePoint>());
      }
      
      // Cluster the points.
      for (DoublePoint input : inputs) {
        // Find the nearest cluster.
        tempPoint = null;
        
        for (DoublePoint key : results.keySet()) {
          if (tempPoint == null || input.distance(key) < input.distance(tempPoint)) {
            tempPoint = key;
          }
        }
        
        results.get(tempPoint).add(input);
      }
      
      // Recompute the clusters as an average.
      newClusters = new ArrayList<DoublePoint>();
      
      for (DoublePoint cluster : clusters) {
        tempPoint = DoublePoint.average(results.get(cluster));
        newClusters.add(tempPoint);
        double delta = Math.abs(cluster.distance(tempPoint));
        if (delta > EPSILON && !changed) {
          changed = true;
        }
      }
      
      clusters = new ArrayList<DoublePoint>(newClusters);
      iterations++;
    }
    
    System.out.println("Number of iterations: " + iterations);
    return clusters;
  }
}
