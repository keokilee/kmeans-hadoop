package edu.hawaii.ics621.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class EchoOnce extends Configured implements Tool {
  /**
   * Fetches strings and reverses
   */
    public static class Map extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

        private Text inputText   = new Text();
        private Text reverseText = new Text();

        public void map(LongWritable key, Text inputs,
                        OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {

        String inputString = inputs.toString();
        int length = inputString.length();
        StringBuffer reverse = new StringBuffer();
        for(int i=length-1; i>=0; i--)
        {
          reverse.append(inputString.charAt(i));
        }
        inputText.set(inputString);
        reverseText.set(reverse.toString());
        output.collect(inputText,reverseText);
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
    System.out.println("EchoOhce [-m nmaps] [-r nreduces] <inputs> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), EchoOnce.class);
    conf.setJobName("EchoOhce");

    // the keys are strings
    conf.setOutputKeyClass(Text.class);
    // the values are reverse strings
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
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
    return 0;
  }


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new EchoOnce(), args);
    System.exit(res);
  }

}