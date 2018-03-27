import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TitanicDataAnalysis_Survived {

	public static class SurvivedMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text people = new Text();
		private final static IntWritable one = new IntWritable(1);
	    public void map(LongWritable key, Text record, Context context ) throws IOException, InterruptedException { 
	      String[] person = record.toString().split(",");
	      if(person.length>6){
	    	  String survived = person[1] + " " + person[4] + " " + person[5];
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
