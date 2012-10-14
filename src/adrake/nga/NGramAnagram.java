package adrake.nga;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class NGramAnagram {

    private static String TO_FIND = "adrake.nga.toFind";
    
    private static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>
    {
        private String toFind;
        
        private static String sortChars(String s)
        {
            StringBuilder s2 = new StringBuilder();
            char[] sChars = s.replaceAll(" ", "").toCharArray();
            for (int i = 0; i < sChars.length; i++) {
                if (Character.isLetter(sChars[i]))
                    s2.append(sChars[i]);
            }
            char[] sChars2 = s2.toString().toCharArray();
            Arrays.sort(sChars2);
            return new String(sChars2);
        }
        
        public void configure(JobConf conf)
        {
            super.configure(conf);
            toFind = sortChars(conf.get(TO_FIND));
        }
        
        public void map(LongWritable key, Text value,
                OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException
        {
            String[] gibs = value.toString().split("\t");
            String ngram = gibs[0];
            if (toFind.equals(sortChars(ngram))) {
                output.collect(new Text(ngram), new LongWritable(Long.parseLong(gibs[2])));
            }
        }
    }
    
    private static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterator<LongWritable> values,
                OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException
        {
            long sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new LongWritable(sum));
        }
    }
    
    public static void main(String[] args) throws IOException
    {
        if (args.length != 3) {
            System.err.println("Usage: NGramAnagram <in> <out> <string>");
            System.exit(1);
        }
        
        JobConf conf = new JobConf(NGramAnagram.class);
        conf.setJobName("ngramanagram-" + args[2]);
        conf.set(TO_FIND, args[2]);
        
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient jc = new JobClient(conf);
        conf.setNumReduceTasks((int)(jc.getClusterStatus().getTaskTrackers() * conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 2) * 0.95));
        
        JobClient.runJob(conf);
    }
}
