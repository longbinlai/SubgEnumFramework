package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import dbg.hadoop.subgraphs.io.DoubleIntegerPairComparator;
import dbg.hadoop.subgraphs.io.DoubleIntegerPairGroupComparator;
import dbg.hadoop.subgraphs.io.DoubleIntegerPairPartitioner2;
import dbg.hadoop.subgraphs.io.DoubleIntegerPairWritable;

public class PrepareDataS1Driver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// The parameters: <degreefile> <graphfile> <outputDir> <numReducers> <jarFile>
		String degreeFile = args[0];
		String graphFile = args[1];
		
		int numReducers = Integer.parseInt(args[3]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		//conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
		
		Job job = new Job(conf, "PrepareData Stage One");
		((JobConf)job.getConfiguration()).setJar(args[4]);

		job.setReducerClass(PrepareDataS1Reducer.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapOutputKeyClass(DoubleIntegerPairWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(DoubleIntegerPairComparator.class);
		job.setGroupingComparatorClass(DoubleIntegerPairGroupComparator.class);
		job.setPartitionerClass(DoubleIntegerPairPartitioner2.class);
		
		job.setNumReduceTasks(numReducers);
		
		MultipleInputs.addInputPath(job, 
				new Path(degreeFile),
				SequenceFileInputFormat.class,
				PrepareDataDegreeMapper.class);
		
		MultipleInputs.addInputPath(job, 
				new Path(graphFile),
				KeyValueTextInputFormat.class,
				PrepareDataS1Mapper.class);
		
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
		return 0;
	}
}
