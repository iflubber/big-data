import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordsWithSameStartEnd {

	public static class WordCheckMapper extends Mapper<LongWritable, Text, Text, IntWritable>{ 

	    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException { 
	      StringTokenizer itr = new StringTokenizer(value.toString()); 
	      while (itr.hasMoreTokens()) { 
	    	  String word = itr.nextToken();
	    	  if((word.charAt(0) == word.charAt(word.length()-1))&&(word.length()>1))
	    		  context.write(new Text(word), new IntWritable(1));
	    }
	  }
	}
	
	public static class WordCheckReducer extends Reducer<Text,IntWritable,Text,IntWritable> { 
	    private IntWritable result = new IntWritable(); 

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
	      int count = 0;  
	      for (IntWritable val : values) { 
	        count += val.get();  
	      }
	      result.set(count);     
	      context.write(key, result); 
	  }
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "words with same start & end");
	  
	    job.setJarByClass(WordsWithSameStartEnd.class);
	    job.setMapperClass(WordCheckMapper.class);
	    job.setReducerClass(WordCheckReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
