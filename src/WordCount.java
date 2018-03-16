import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {  // we are taking a class by name WordCount

//Mapper Programme

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{ 
//we are extending the Mapper default class having the arguments keyIn as LongWritable and ValueIn as Text and KeyOut as Text and ValueOut as IntWritable

    private final static IntWritable one = new IntWritable(1); //we are declaring a IntWritable variable ‘one’ with value as 1

    private Text word = new Text(); //we are declaring a Text variable ‘word’ to store the output keys

    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException { //we are overriding the map method which will run one time for every line
      StringTokenizer itr = new StringTokenizer(value.toString()); //we are storing the line in a string tokenizer variable itr
      while (itr.hasMoreTokens()) {  //we have given an while condition on the variable itr if it as one or more tokens then it will enter the while loop.
        word.set(itr.nextToken());   //Assign each word from the tokenizer(of String type) to a Text word
        context.write(word, one);    //Form key value pairs for each word as <word,one> and push it to the output context
      }
    }
  }


//Reducer Program
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> { //extends the default Reducer class with arguments KeyIn as Text and ValueIn as IntWritable which are same as the outputs of the mapper class and KeyOut as Text and ValueOut as IntWritable which will be final outputs of our MapReduce program.
    private IntWritable result = new IntWritable(); //we are declaring a IntWritable variable result which will store the number of occurrences of a word in the input dataset file

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
//we are overriding the Reduce method which will run each time for every key

      int sum = 0;  //we are declaring a variable ‘sum’ of type intWritable and Initialized as 0. Which will store the sum of all the individual repeated words into it.
      for (IntWritable val : values) { 
//a foreach loop is taken which will run each time for the values inside the “Iterable values” which are coming from the shuffle and sort phase after the mapper phase. We are taking another variable as ‘val’ which will be incremented every time as many values are there for that key
        sum += val.get();  //Iterate through all the values with respect to a key and sum up all     of them
      }
      result.set(sum);     //we are storing the sum of the values in ‘result’ variable.
      context.write(key, result); //Form key value pairs for each word as <key,result> and push it to the output context
    }
  }

//Main method
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
  
    job.setJarByClass(WordCount.class);
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
