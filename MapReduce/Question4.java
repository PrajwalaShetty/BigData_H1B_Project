import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
public class Question4 {

	public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		
		LongWritable one = new LongWritable(1);
	    public void map(LongWritable key, Text value, Context context) throws IOException,
	    InterruptedException {
	        if (key.get() > 0)

	        {
	            String[] token = value.toString().split("\t");
	            if (!token[1].equals("NA") && token[1] != null && !token[2].equals("NA") && token[2] != null && !token[7].equals("NA") && token[7] != null) {
	                Text answer = new Text(token[2].replaceAll("\"", "") + "\t" + token[7]);
	                context.write(answer, one);
	            }
	        }
	    }
	}
	
	public static class MyPartitioner extends Partitioner < Text, LongWritable > {
	    @Override
	    public int getPartition(Text key, LongWritable value, int numReduceTasks) {
	        String[] str = key.toString().split("\t");
	        if (str[1].equals("2011"))
	            return 0;
	        if (str[1].equals("2012"))
	            return 1;
	        if (str[1].equals("2013"))
	            return 2;
	        if (str[1].equals("2014"))
	            return 3;
	        if (str[1].equals("2015"))
	            return 4;
	        if (str[1].equals("2016"))
	            return 5;
	        else
	            return 6;


	    }
	}
	public static class MyReducer extends Reducer< Text, LongWritable, NullWritable, Text > {
	    private TreeMap < LongWritable,Text > Top5Employers = new TreeMap < LongWritable,Text > ();
	    long sum = 0;
	    public void reduce(Text key, Iterable < LongWritable > values, Context context) throws IOException,
	    InterruptedException {
	        sum = 0;
	        for (LongWritable val: values) {
	            sum += val.get();
	        }
	        Top5Employers.put(new LongWritable(sum), new Text(key + "," + sum));
	        if (Top5Employers.size() > 5)
	            Top5Employers.remove(Top5Employers.firstKey());

	    }
	    protected void cleanup(Context context) throws IOException,
	    InterruptedException {
	        for (Text t: Top5Employers.descendingMap().values())
	            context.write(NullWritable.get(), t);

	    }


	}


	    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException

	    {
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Top  5 Employers");

	        job.setJarByClass(Question4.class);
	        job.setMapperClass(MyMapper.class);
	        job.setPartitionerClass(MyPartitioner.class);
	        job.setReducerClass(MyReducer.class);

	        job.setNumReduceTasks(7);

	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(LongWritable.class);

	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);

	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);

	    }

	}
	