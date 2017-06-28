

import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib. input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class HAsignFourOne
{

public static class Map extends Mapper<LongWritable, Text, Text, Text>
{


	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{

			//split method is used to split the strings into array
			String[] lineArray = value.toString().split("\\|");
			//Fetching company name and product name form linearray
			Text CompanyName = new Text(lineArray[0].toString());

         		Text ProductName = new Text(lineArray[1].toString());
			
			//checking the condition  of not available
		
	if (CompanyName.toString().matches("NA") || ProductName.toString().matches("NA")){

			Text type = new Text("NA");

			
			//used to send the output of mapper class
			context.write(type,value);

		}
				

				
			
	
			
		
		
		
	}
}




	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 

	{



Configuration conf = new Configuration();

Job job = new Job(conf,"MapReduce-1");

job.setJarByClass(HAsignFourOne.class);


job.setMapOutputKeyClass(Text.class);

job.setMapOutputValueClass(Text.class);


job.setMapperClass(Map.class);


job.setInputFormatClass(TextInputFormat.class);

job.setOutputFormatClass(TextOutputFormat.class);


FileInputFormat.addInputPath(job, new Path(args[0]));

FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);

	}

}

