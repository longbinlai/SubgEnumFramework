package dbg.hadoop.subgenum.frame;

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoCodec;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Graph;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.CliqueEncoder;
import dbg.hadoop.subgraphs.utils.Utility;

public class EnumClique {
	private static InputInfo inputInfo  = new InputInfo();
	public static String workdir="";
	public static String filename="";

	public static void main(String[] args) throws Exception {
		inputInfo = new InputInfo(args);
		boolean isCountOnly = inputInfo.isCountOnly;
		String workDir = inputInfo.workDir;
		
		if (workDir.toLowerCase().contains("hdfs")) {
			int pos = workDir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(workDir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		Configuration conf = new Configuration();
		conf.setBoolean("count.only", isCountOnly);
		conf.setStrings("clique.number.vertices", inputInfo.cliqueNumVertices);
		if(!isCountOnly){
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
					.toString() + "/" + Config.cliques), conf);
		}
		
		// Delete existed output
		if(Utility.getFS().isDirectory(new Path(workDir + "frame.clique.res"))){
			Utility.getFS().delete(new Path(workDir + "frame.clique.res"));
		}
		if(Utility.getFS().isDirectory(new Path(workDir + "frame.clique.cnt"))){
			Utility.getFS().delete(new Path(workDir + "frame.clique.cnt"));
		}
		
		long startTime=System.currentTimeMillis();   
		String[] opts = { workDir + "triangle.res", workDir + "frame.clique.res",	
					inputInfo.numReducers, inputInfo.jarFile, inputInfo.cliqueNumVertices};
		
		ToolRunner.run(conf, new EnumCliqueDriver(), opts);
		System.out.println("End of Enumeration");
		
		long endTime = System.currentTimeMillis();
		System.out.println(" " + (endTime - startTime) / 1000 + "s");
		
		if (inputInfo.isCountPatternOnce) {
			String[] opts2 = { workDir + "frame.clique.res", workDir + "frame.clique.cnt", 
					inputInfo.numReducers, inputInfo.jarFile, inputInfo.cliqueNumVertices };
			if(inputInfo.isCountOnly)
				ToolRunner.run(conf, new GeneralPatternCountDriver(CliqueCountMapper1.class), opts2);
			else
				ToolRunner.run(conf, new GeneralPatternCountDriver(CliqueCountMapper2.class), opts2);	
		}
		
		//if(isCountOnly){
			//Utility.getFS().delete(new Path(workDir + "frame.clique.cnt"));
			//Utility.getFS().delete(new Path(workDir + "frame.clique.res"));
		//}
		//else {
		//	Utility.getFS().delete(new Path(workDir + "frame.clique.cnt"));
		//}
	}
}

class EnumCliqueDriver extends Configured implements Tool{
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <triangleDir> <outputDir> <numReducers> <jarFile> <cliqueNumVertices>
		int numReducers = Integer.parseInt(args[2]);
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
		Job job = new Job(conf, "Frame " + args[4] + "-Clique");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		job.setMapperClass(EnumCliqueMapper.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(HVArray.class);
		job.setOutputKeyClass(LongWritable.class);
		if(conf.getBoolean("count.only", true)){
			job.setOutputValueClass(LongWritable.class);
			job.setReducerClass(EnumCliqueCountReducer.class);
		}
		else{
			job.setOutputValueClass(HVArray.class);
			job.setReducerClass(EnumCliqueEnumReducer.class);
		}
		
		job.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}

class EnumCliqueMapper extends
	Mapper<NullWritable, HVArray, LongWritable, HVArray> {
	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(new LongWritable(_value.getFirst()),
				new HVArray(_value.getSecond(), _value.getLast()));
	}
}

class EnumCliqueCountReducer extends 
	Reducer<LongWritable,HVArray, LongWritable, LongWritable> {

	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values, Context context) throws IOException, InterruptedException{
		Graph G = new Graph();
		int cnt=0;
		for (HVArray val : values) {
			cnt = cnt + 1;
			for (int i = 0; i < val.size(); i = i + 2) {
				G.addEdge(val.get(i), val.get(i + 1));
			}
		}
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("clique.number.vertices"));
		long numclique = G.countCliquesOfSize(k - 1);
		context.write(_key,new LongWritable(numclique));
	}
}

class EnumCliqueEnumReducer extends
		Reducer<LongWritable, HVArray, LongWritable, HVArray> {
	private static TLongLongHashMap cliqueMap = null;
	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		Graph G = new Graph();
		int cnt = 0;
		for (HVArray val : values) {
			cnt = cnt + 1;
			for (int i = 0; i < val.size(); i = i + 2) {
				G.addEdge(val.get(i), val.get(i + 1));
			}
		}
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("clique.number.vertices"));
		long[] cliqueEnc = G.enumCliqueOfSize(k - 1, _key.get(), cliqueMap);
		context.write(_key, new HVArray(cliqueEnc));
	}
	
	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		for (int i = 0; i < paths.length; ++i) {
			if (paths[i].toString().contains("part-r-")) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[i], conf);
				LongWritable key = null;
				HVArray val = null;
				try {
					key = (LongWritable) reader.getKeyClass().newInstance();
					val = (HVArray) reader.getValueClass().newInstance();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
				while(reader.next(key, val)){
					for(long v : val.toArrays()){
						cliqueMap.put(v, key.get());
					}
				}
				reader.close();
				
			}
		}
	}
}

class CliqueCountMapper1 extends
		Mapper<LongWritable, LongWritable, NullWritable, LongWritable> {
	@Override
	public void map(LongWritable _key, LongWritable _value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), _value);
	}
}

class CliqueCountMapper2 extends
		Mapper<LongWritable, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(LongWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), new LongWritable(
				CliqueEncoder.getNumCliquesFromEncodedArray(_value.toArrays())));
	}
}



