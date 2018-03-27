import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TitanicDataAnalysis_AvgAge {

	public static class AgeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{ 
	    public void map(LongWritable key, Text record, Context context ) throws IOException, InterruptedException { 
	      String[] person = record.toString().split(",");
	      if(person.length>6){
		      String sex = person[4];
		      if(person[1].equals("1")){
			      try{
			    	  Integer age = Integer.parseInt(person[5]);
			    	  context.write(new Text(sex), new IntWritable(age));
			      }catch(NumberFormatException nfe){
			    	  //missing age data
			    }
		      }
	      }
	    }
	  }
	
	public static class AgeReducer extends Reducer<Text,IntWritable,Text,FloatWritable> { 
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
	    	Float total = (float) 0;
	    	int count = 0;
	    	for(IntWritable value : values){
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
	  
	    job.setJarByClass(TitanicDataAnalysis_AvgAge.class);
	    job.setMapperClass(AgeMapper.class);
	    job.setReducerClass(AgeReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
