import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TitanicDataAnalysis_Survived {

	public static class SurvivedMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text people = new Text();
		private final static IntWritable one = new IntWritable(1);
	    public void map(LongWritable key, Text record, Context context ) throws IOException, InterruptedException { 
	      String[] person = record.toString().split(",");
	      String survived = null;
	      if(person.length>6){
	    	  if(person[1].equals("0"))
	    		  survived = "Survived " + person[2] + " " + person[4] + " " + person[5];
	    	  else if(person[1].equals("1"))
	    		  survived = "Died " + person[2] + " " + person[4] + " " + person[5];
	    	  people.set(survived);
	    	  context.write(people, one);
	      }
	     }
	}
	
	public static class SurvivedReducer extends Reducer<Text,IntWritable,Text,IntWritable> { 
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
	    	int sum = 0;
	    	for(IntWritable value : values){
	    		sum +=value.get();
	    	}
	    	context.write(key, new IntWritable(sum));
	  }
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Titanic Data Analysis");
	  
	    job.setJarByClass(TitanicDataAnalysis_Survived.class);
	    job.setMapperClass(SurvivedMapper.class);
	    job.setReducerClass(SurvivedReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
