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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordsWithVowels {
	
	public static class VowelCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{ 

	    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException { 
	      StringTokenizer itr = new StringTokenizer(value.toString()); 
	      while (itr.hasMoreTokens()) { 
	    	  String word = itr.nextToken();
	    	  char[] charArray = word.toCharArray();
	    	  for(char c: charArray){
	    		  if(c == 'a' || c == 'A')
	    			  context.write(new Text("A|a"), new IntWritable(1));
	    		  if(c == 'e' || c == 'E')
	    			  context.write(new Text("E|e"), new IntWritable(1));
	    		  if(c == 'i' || c == 'I')
	    			  context.write(new Text("I|i"), new IntWritable(1));
	    		  if(c == 'o' || c == 'O')
	    			  context.write(new Text("O|o"), new IntWritable(1));
	    		  if(c == 'u' || c == 'U')
	    			  context.write(new Text("U|u"), new IntWritable(1));
	    			  
	    	  }
	    }
	  }
	}
	
	public static class VowelCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> { 
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
	    Job job = Job.getInstance(conf, "word with vowels count");
	  
	    job.setJarByClass(WordsWithVowels.class);
	    job.setMapperClass(VowelCountMapper.class);
	    job.setReducerClass(VowelCountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
