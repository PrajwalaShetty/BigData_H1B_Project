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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Question3 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException
		{
			String[] str = value.toString().split("\t");
			String soc_name = str[3];
			String job = str[4];
			int count = 0;
			if(job.contains("DATA SCIENTIST"))
				count++;
			c.write(new Text(soc_name), new IntWritable(count));
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		String opkey="";
		int max = 0;
		
		public void reduce(Text key, Iterable<IntWritable> values, Context c) throws IOException, InterruptedException
		{
		
		int count = 0;
		

		for(IntWritable val: values)
		{
			count+=val.get();
			
			if (max<count)
			{
				max = count;
				
				opkey = key.toString();
			}
		}
			
		
		}
		
		public void cleanup(Context c) throws IOException, InterruptedException
		{
		
			c.write(new Text(opkey), new IntWritable(max));
		}
	
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Industry with most Data Scientist Positions");
		    job.setJarByClass(Question3.class);
		    job.setMapperClass(MyMapper.class);
		    job.setReducerClass(MyReducer.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}