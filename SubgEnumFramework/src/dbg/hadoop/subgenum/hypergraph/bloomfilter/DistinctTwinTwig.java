package dbg.hadoop.subgenum.hypergraph.bloomfilter;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoCodec;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

public class DistinctTwinTwig{
	
	private static Logger log = Logger.getLogger(DistinctTwinTwig.class);
	
	public static int main(String[] args) throws Exception{
		return run(new InputInfo(args));
	}
	
	public static int run(InputInfo inputInfo) throws Exception{	
		String numReducers = inputInfo.numReducers;
		String inputFilePath = inputInfo.inputFilePath;
		String jarFile = inputInfo.jarFile;
		String workDir = inputInfo.workDir;
		
		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		if (workDir.toLowerCase().contains("hdfs")) {
			int pos = workDir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(workDir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		String input = workDir + Config.adjListDir + ".0";
		String output = workDir + Config.distinctTwinTwigDir;
		
		if(Utility.getFS().isDirectory(new Path(output))){
			Utility.getFS().delete(new Path(output));
		}
		
		log.info("Calculating distinct TwinTwig...");
		// The parameters: <inputDir> <outputDir> <numReducers> <jarFile>
		String opts[] = {input, output, numReducers, jarFile};		
		ToolRunner.run(new Configuration(), new DTTDriver(), opts);
		
		return 0;

	}
		
}

class DTTDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <inputDir> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "DistinctTwinTwig");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		job.setReducerClass(DTTReducer.class);
		job.setCombinerClass(DTTReducer.class);
		
		job.setOutputKeyClass(HVArray.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(HVArrayComparator.class);
		//job.setGroupingComparatorClass(HVArrayGroupComparator.class);

		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//if(enableOutputCompress){
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType
								(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		job.setNumReduceTasks(numReducers);
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		MultipleInputs.addInputPath(job, 
				new Path(args[0]),
				SequenceFileInputFormat.class,
				DTTMapper.class);

		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}



class DTTMapper extends Mapper<LongWritable, HyperVertexAdjList, HVArray, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	// The hypervertex set
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context) 
			throws IOException, InterruptedException{
		long[] largeVertices = value.getLargeDegreeVertices();
		
		int len = largeVertices.length;
		for(int i = 0; i < len; ++i){
			for(int j = i + 1; j < len; ++j){
				context.write(new HVArray(largeVertices[i],  largeVertices[j]), one);
			}
		}
	}
}

class DTTReducer extends Reducer<HVArray, IntWritable, HVArray, IntWritable> {
	@Override
	public void reduce(HVArray _key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {	
		int sum = 0;
		for(IntWritable value : values){
			sum += value.get();
		}
		context.write(_key, new IntWritable(sum));
	}
}

