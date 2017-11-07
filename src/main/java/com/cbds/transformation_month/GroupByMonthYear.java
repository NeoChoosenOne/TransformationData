package com.cbds.transformation_month;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupByMonthYear{
  
  /*Class extends from Mapper Class Input params (Object, Text) usually Object Key is the offset of bytes 
   * and Text are the lines at input text, Output params (Text, IntWriteable).
   */

  public static class Map extends Mapper<Object, Text, Text, DoubleWritable>{
    //Method implementation for map function, input params (key,value) as types (Object, Text) and ouput values as context as type Context.
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //Array object to store each column in the input file that is delimited by the character \u0001 (Start of Header).
      String [] values  = value.toString().split("\u0001");
      //Array object to store eache componet of the date
      String [] valores = values[2].substring(3).split("/");
      //String object that create a key with form like MMYYYY.
      String new_key = valores[1] + valores[0];
      //Output of the form <Text, DoubleWritable>
      context.write(new Text(new_key), new DoubleWritable(new Double(values[5])));
    }
  }
  /*Class extends from Reducer Class input 
   * 
   */
  public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	//Method implementation for reduce function, input params (key,value) as types (Text, Iterable<DoubleWritable>) and ouput values as context as type Context.
	public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		//primitive variable to make the total sum of the sales in the file. 
		double suma_sales = 0;
		//Loop to get each member of the Iterator object.
		while(values.iterator().hasNext()){
			//Sum of the sales with the same key.
			suma_sales += new Double(values.iterator().next().toString()).doubleValue();
		}
		//Output of the form <Text, DoubleWritable>
		context.write(key, new DoubleWritable(suma_sales));
    }
  }
  //This example takes by default the inputTextFormat in input split.
  public static void main(String[] args) throws Exception {  
	//Hadoop configuration 
    Configuration conf = new Configuration();
    //Property for the configuration of hadoop to delimited the output file by commas.
    conf.set("mapred.textoutputformat.separator", ",");
    //Job Configuration set it a hadoop conf and Job name.
    Job job = Job.getInstance(conf, "group_mothyear");
    //Set to job configuration the main class that contains main method.
    job.setJarByClass(GroupByMonthYear.class);
    //Set to job configuration the class where the Mapper Implementation is.
    job.setMapperClass(Map.class);
    //Set to job configuration the class where the Combiner Implementation is.
    job.setCombinerClass(Reduce.class);
    //Set to job configuration the class where the Reducer Implementation is.
    job.setReducerClass(Reduce.class);
    //Set to job configuration the class 
    job.setOutputKeyClass(Text.class);
    //Set to job configuration the class 
    job.setOutputValueClass(DoubleWritable.class);
    //Input path in HDFS to read files to InputSpliter and Record Reader 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    //Output path in HDFS to put output result for this job
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //Wait until Job workflow finish.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
