package edu.hawaii.ics621.examples;

/**
 * This example prepared for
 *
 * Brandeis University
 * cs147a
 * Spring 2008
 *
 * MultiFetch accepts one or more URL strings as input and outputs an
 * (url, title) pair for each one, where "title" is a string
 * containing the text between the html title tags.
 *
 * Outputs errors with System.err.println, which can be found in the
 * logs/userlogs/[map_id]/stderr directory under your root hadoop
 * directory.
 *
 * This class is a modification of the WordCount.java example included
 * with Hadoop in src/examples/org/apache/hadoop/examples/
 */

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.regex.MatchResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is an example Hadoop Map/Reduce application. It fetches the
 * web page at each input URL and extracts the title. The output is a
 * single tuple for each input URL: the URL and its associated title.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar multifetch
 *         [-m <i>maps</i>] [-r <i>reduces</i>] <i>urls</i> <i>out-dir</i> 
 */
public class HadoopMultiFetch extends Configured implements Tool {
  /**
   * Fetches web pages and extracts their titles.
   */
    public static class Map extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

        private Text urlText   = new Text();
        private Text titleText = new Text();
        
        public void map(LongWritable key, Text urls,
                        OutputCollector<Text, Text> output, 
                        Reporter reporter) throws IOException {
            StringTokenizer itr = new StringTokenizer(urls.toString());
            while(itr.hasMoreTokens()) {
                String surl            = itr.nextToken();
                try {
                    URL url            = new URL(surl);
                    URLConnection conn = url.openConnection();
                    Scanner scanner    = new Scanner(conn.getInputStream());
                    scanner.findInLine("<title>([^<]+)</title>");
                    MatchResult match  = scanner.match();
                    titleText.set(match.group(1));
                    scanner.close();
                    urlText.set(surl);
                    output.collect(urlText, titleText);
                }
                catch(MalformedURLException e) {
                    System.err.println("Malformed URL: " + surl);
                }
                catch(IllegalStateException e) {
                    System.err.println("URL " + surl + " has no title");
                }
                catch(IOException e) {
                    System.err.println("Cannot open " + surl + " (" +
                                       e.toString() + ")");
                }
            }
        }
    }
  
    /**
     * A reducer class that just emits its input.
     */
    public static class Reduce extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, 
                           Reporter reporter) throws IOException {
            while (values.hasNext()) {
                output.collect(key, values.next());
            }
        }
    }
  
  static int printUsage() {
    System.out.println("multifetch [-m nmaps] [-r nreduces] <inputs> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), HadoopMultiFetch.class);
    conf.setJobName("multifetch");
 
    // the keys are urls (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are titles (strings)
    conf.setOutputValueClass(Text.class);
    
    conf.setMapperClass(Map.class);        
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
                         other_args.size() + " instead of 2.");
      return printUsage();
    }
    
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
    JobClient.runJob(conf);
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopMultiFetch(), args);
    System.exit(res);
  }

}
