package mercier_daoud.mapReduce;
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

/*Map Reduce 2
 *  Count number of first name by number of origin
 *   Ex : How many first name has x origins ?
 * 
 * Output each numbers of origins possible with the number of first name that have this number
 */

public class NumberByOrigin {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text numberOfOrigins = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
		// Split each input by ";" and retrieve the splitted string in an Array
    	String[] parts = value.toString().split(";");
		// Remove all the space in the string

	    String origins = parts[2].replaceAll("\\s+","");
	    
		// Split the origin by coma ","

	    StringTokenizer itr = new StringTokenizer(origins, ",");
	    
	    //Count the number of origins (number of token)
	    numberOfOrigins.set(Integer.toString(itr.countTokens()));
		// Each number of origins is a key with a value 1
	    context.write(numberOfOrigins, one);
	   
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
	//Reduce with a summation of the value for each key

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
    job.setJarByClass(NumberByOrigin.class);
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