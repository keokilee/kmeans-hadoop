package edu.hawaii.ics621.algorithms;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import edu.hawaii.ics621.DoublePoint;

/**
 * MapReduce implementation of the KMeansClustering algorithm using Hadoop.
 * 
 * @author George Lee
 *
 */
public class MapReduceKMeans extends Configured implements Tool {
  public static final String KEY_PREFIX = "kmeans.centers.";
  private static final String dataPath = "/user/kmeans/data";
  private static final String[] dataPaths = {"data-1m.txt", "data-10m.txt", "data-100m.txt"};
  private static final String[] clusterPaths = {"centers-10.txt", "centers-100.txt", "centers-1000.txt"};
//  private static final int MAX_ITERATIONS = 1000;
//  private static final double EPSILON = 1E-8;
  
  /**
   * Handles the Map phase of the algorithm. Takes the input and assigns it to a cluster.
   */
  public static class Map extends MapReduceBase implements
      Mapper<Text, Text, IntWritable, Text> {
    
    private List<DoublePoint> centers;
    
    /**
     * Get the centers from the configuration.
     */
    @Override
    public void configure(JobConf conf) {
      super.configure(conf);
      
      if (this.centers == null) {
        this.centers = new ArrayList<DoublePoint>();
        String[] coords;
        DoublePoint point;
        
        // Load the centers from the conf.
        int count = Integer.parseInt(conf.get(KEY_PREFIX + "count"));
        
        for (int i=0; i < count; i++) {
          coords = conf.get(KEY_PREFIX + i).split(",");
          point = new DoublePoint(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
          centers.add(point);
        }
      }
    }
    
    /**
     * Map the input to a cluster.
     */
    @Override
    public void map(Text key, Text input,
        OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      
      // Parse the input in the format x,y.
      String[] coords = key.toString().split(",");
      DoublePoint point = new DoublePoint(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
      
      double minDist = Double.MAX_VALUE;
      int minIndex = 0;
      int index = 0;
      
      // Assign the input to a cluster.
      for (DoublePoint center : centers) {
        if (center.distance(point) < minDist) {
          minDist = center.distance(point);
          minIndex = index;
        }
        
        index++;
      }
      
      // Emit the output as key, value.
      output.collect(new IntWritable(minIndex), key);
    }
  }
  
  /**
   * Combiner class that calculates partial sums for the reducer.
   */
  public static class Combiner extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {
      // Track the partial sums and the count
      double x = 0;
      double y = 0;
      int count = 0;
      
      String[] coords;
      while (values.hasNext()) {
        coords = values.next().toString().split(",");
        x += Double.parseDouble(coords[0]);
        y += Double.parseDouble(coords[1]);
        count += 1;
      }
      
      // Construct the value to be x dimension, y dimension, and count
      String outString = Double.toString(x) + "," + Double.toString(y) + "," + count;
      output.collect(key, new Text(outString));
    }
  }
  
  /**
   * A reducer class that takes results from the combiner to construct the input.
   */
  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    
    private List<DoublePoint> centers;
    
    /**
     * Get the centers from the configuration.
     */
    @Override
    public void configure(JobConf conf) {
      super.configure(conf);
      if (this.centers == null) {
        this.centers = new ArrayList<DoublePoint>();
        String[] coords;
        DoublePoint point;
        
        // Load the centers from the conf.
        int count = Integer.parseInt(conf.get(KEY_PREFIX + "count"));
        
        for (int i=0; i < count; i++) {
          coords = conf.get(KEY_PREFIX + i).split(",");
          point = new DoublePoint(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
          centers.add(point);
        }
      }
    }
    
    /**
     * Calculate the new averages for the clusters.
     */
    @Override
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {
      // We receive the sums from the combiner, so we have to sum up those values and count.
      double x = 0;
      double y = 0;
      int count = 0;
      String[] partialSums;
      
      while (values.hasNext()) {
        partialSums = values.next().toString().split(",");
        x += Double.parseDouble(partialSums[0]);
        y += Double.parseDouble(partialSums[1]);
        count += Integer.parseInt(partialSums[2]);
      }
      
      // Calculate the averages to find the new cluster value.
      x /= count;
      y /= count;
      String outStr = Double.toString(x) + "," + Double.toString(y);
      
      // Check if we need to update our center.
      
      output.collect(null, new Text(outStr));
    }
  }

  static int printUsage() {
    System.out.println("MapReduceKMeans <inputs> <clusters> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  private void createJob(String jobName, String dataPath, String clusterPath, String outputPath, int numMappers) throws Exception {
    JobConf conf = new JobConf(getConf(), MapReduceKMeans.class);
    
    conf.setJobName(jobName);

    // the keys are strings.
    conf.setInputFormat(KeyValueTextInputFormat.class);
    
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Combiner.class);
    conf.setReducerClass(Reduce.class);
    
    conf.setNumMapTasks(numMappers);
    FileInputFormat.setInputPaths(conf, new Path(dataPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    
    // Read the clusters file to generate the clusters.
    FileSystem fs = FileSystem.get(conf);
    DataInputStream stream = new DataInputStream(fs.open(new Path(clusterPath)));
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    String line;
    int index = 0;
    while ((line = reader.readLine()) != null) {
      conf.set(KEY_PREFIX + index, line);
      index++;
    }
    
    conf.set(KEY_PREFIX + "count", Integer.toString(index));
    
    // Write out the clusters
    stream.close();
    
    long start = System.currentTimeMillis();
    JobClient.runJob(conf);
    long end = System.currentTimeMillis();
    
    System.out.println(conf.getJobName() + "took " + ((end - start) / 1000) + " seconds");
    
  }
  
  @Override
  public int run(String[] args) throws Exception {
    String jobName;
    String fullDataPath;
    String fullClusterPath;
    String outputPath;
    
    // Create jobs varying data.
    for (String path : dataPaths) {
      jobName = "kmeans-" + path.split(".")[0];
      fullDataPath = dataPath + "/" + path;
      fullClusterPath = dataPath + "/centers-1000.txt";
      outputPath = "/user/kmeans/output/" + jobName;
      this.createJob(jobName, fullDataPath, fullClusterPath, outputPath, 10);
    }
    
    // Create jobs varying clusters.
    for (String path : clusterPaths) {
      jobName = "kmeans-" + path.split(".")[0];
      fullDataPath = dataPath + "/data-100m.txt";
      fullClusterPath = dataPath + "/" + path;
      outputPath = "/user/kmeans/output/" + jobName;
      this.createJob(jobName, fullDataPath, fullClusterPath, outputPath, 10);
    }
    
    // Create jobs varying mappers.
    for (int i=5; i <= 20; i += 5) {
      jobName = "kmeans-n-" + i;
      fullDataPath = dataPath + "/data-100m.txt";
      fullClusterPath = dataPath + "/centers-1000.txt";
      outputPath = "/user/kmeans/output/" + jobName;
      this.createJob(jobName, fullDataPath, fullClusterPath, outputPath, i);
    }
    
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MapReduceKMeans(), args);
    System.exit(res);
  }

}