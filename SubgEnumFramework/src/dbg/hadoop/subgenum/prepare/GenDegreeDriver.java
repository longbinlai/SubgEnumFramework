package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

public class GenDegreeDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// The parameters: <inputfile> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		//conf.setBoolean("graph.undirected", Boolean.parseBoolean(args[4]));
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		//conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
		
		Job job = new Job(conf, "Degree Generation");
		((JobConf)job.getConfiguration()).setJar(args[3]);

		job.setMapperClass(GenDegreeMapper2.class);
		job.setReducerClass(GenDegreeReducer.class);
		job.setCombinerClass(GenDegreeReducer.class);
		
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}
