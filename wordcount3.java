package wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class wordcount3 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text(); 

    private HashSet<String> pattens = new HashSet<String>();
    
    @Override
    protected void setup(Context context) throws IOException,
	InterruptedException {
    	URI[] uri = context.getCacheFiles();
		
		Path patternsPath = new Path(uri[0].getPath());
		String filename = patternsPath.getName().toString();
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = null;
		while ((line = br.readLine()) != null) {
			StringTokenizer wordList = new StringTokenizer(line.toString());
			while (wordList.hasMoreTokens()) {
				pattens.add(wordList.nextToken());
			}
		}
		br.close();
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  String token = itr.nextToken();
    	  if (pattens.contains(token)) {
    		  context.write(new Text(token), one);
    	  }
       
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.addCacheFile(new URI("/wordtest/word-patterns.txt"));
    job.setJarByClass(wordcount3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}