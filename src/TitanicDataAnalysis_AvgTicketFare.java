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


public class TitanicDataAnalysis_AvgTicketFare {

	public static class TicketFareMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{ 
	    public void map(LongWritable key, Text record, Context context ) throws IOException, InterruptedException { 
	      String[] person = record.toString().split(",");
	      
	      try{
	    	  String cabin = person[10];
	    	  if(cabin.isEmpty())
	    		  throw new Exception();
	    	  Float fare = Float.parseFloat(person[9]);
	    	  context.write(new Text(cabin), new FloatWritable(fare));
	      }catch(Exception nfe){
	    	  //missing fare data
	      	}
	     }
	}
	
	public static class TicketFareReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> { 
	    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException { 
	    	Float total = (float) 0;
	    	int count = 0;
	    	for(FloatWritable value : values){
	    		total +=value.get();
	    		count++;
	    	}
	    	Float avg = (Float) total / count;
	    	context.write(key, new FloatWritable(avg));
	  }
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Titanic Data Analysis");
	  
	    job.setJarByClass(TitanicDataAnalysis_AvgTicketFare.class);
	    job.setMapperClass(TicketFareMapper.class);
	    job.setReducerClass(TicketFareReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
