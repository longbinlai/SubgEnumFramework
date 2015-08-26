package dbg.hadoop.subgenum.twintwig;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HVArrayGroupComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.TwinTwigGenerator;
import dbg.hadoop.subgraphs.utils.Utility;
import dbg.hadoop.subgraphs.utils.InputInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import com.hadoop.compression.lzo.LzoCodec;


public class ChordalSquare{
	private static InputInfo inputInfo = null;
	public static void main(String[] args) throws Exception{
		inputInfo = new InputInfo(args);
		String numReducers = inputInfo.numReducers;
		String inputFilePath = inputInfo.inputFilePath;
		String jarFile = inputInfo.jarFile;
		float falsePositive = inputInfo.falsePositive;
		boolean enableBF = inputInfo.enableBF;
		int maxSize = inputInfo.maxSize;
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
		
		Configuration conf = new Configuration();
		
		if(enableBF){
			conf.setBoolean("enable.bloom.filter", true);
			conf.setFloat("bloom.filter.false.positive.rate", falsePositive);
			String bloomFilterFileName = "bloomFilter." + Config.EDGE + "." + falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
				.toString() + "/" + Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String stageOneOutput = workDir + "tt.csquare.tmp.1";
		String stageTwoOutput = workDir + "tt.csquare.res";
		
		String[] opts = { workDir + Config.adjListDir + "." + maxSize, stageOneOutput, numReducers, jarFile };
		ToolRunner.run(conf, new ChordalSquareStageOneDriver(), opts);
		
		String[] opts2 = { workDir + Config.preparedFileDir, stageOneOutput, stageTwoOutput, numReducers, jarFile };
		ToolRunner.run(conf, new ChordalSquareStageTwoDriver(), opts2);

		Utility.getFS().delete(new Path(stageOneOutput));
		Utility.getFS().delete(new Path(stageTwoOutput));
	}
}

class ChordalSquareStageOneDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// args: <working dir> <outputDir> <numReducers> <jarFile>
		Configuration conf = getConf();
		int numReducers = Integer.parseInt(args[2]);

		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
		//conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
		
		Job job = new Job(conf, "TwinTwig ChordalSquare Stage One");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		
	    MultipleInputs.addInputPath(job, new Path(args[0]), 
	    		SequenceFileInputFormat.class, ChordalSquareStageOneMapper.class);
		
		job.setReducerClass(ChordalSquareStageOneReducer.class);

		job.setMapOutputKeyClass(HVArray.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(HVArray.class);
		job.setOutputValueClass(HVArray.class);

		job.setSortComparatorClass(HVArrayComparator.class);
		//job.setGroupingComparatorClass(HVArrayGroupComparator.class);
		
		job.setNumReduceTasks(numReducers);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}

class ChordalSquareStageOneMapper extends
		Mapper<LongWritable, HyperVertexAdjList, HVArray, LongWritable> {

	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;

	// The hypervertex set
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context)
			throws IOException, InterruptedException {
		long cur = key.get();
		long[] largerThanCur = value.getLargeDegreeVertices();
		long[] smallerThanCur = value.getSmallDegreeVerticesGroup1();
		long[] fullNeighbors = new long[largerThanCur.length + smallerThanCur.length];
		System.arraycopy(smallerThanCur, 0, fullNeighbors, 0, smallerThanCur.length);
		System.arraycopy(largerThanCur, 0, fullNeighbors, smallerThanCur.length, largerThanCur.length);
		boolean isOutput = true;
		for(int i = 0; i < fullNeighbors.length - 1; ++i){
			for(int j = i + 1; j < fullNeighbors.length; ++j){
				if(enableBF){
					isOutput = bloomfilterOpr.get().test(HyperVertex.VertexID(fullNeighbors[i]), 
							HyperVertex.VertexID(fullNeighbors[j]));
				}
				if(isOutput){
					context.write(new HVArray(fullNeighbors[i], fullNeighbors[j]), key);
				}
			}
		}
		
	}

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we do not load it again in the case.
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		if (enableBF && bloomfilterOpr == null) {
			bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
					"bloom.filter.false.positive.rate", (float) 0.001));
			try {
				bloomfilterOpr.obtainBloomFilter(conf);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class ChordalSquareStageOneReducer
	extends Reducer<HVArray, LongWritable, HVArray, HVArray> {
	private static TLongArrayList ttList = null;
	public void reduce(HVArray _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {	
		ttList.clear();
		for(LongWritable value : values){
			ttList.add(value.get());
		}
		long[] ttArray = ttList.toArray();
		for(int i = 0; i < ttArray.length - 1; ++i){
			for(int j = i + 1; j < ttArray.length; ++j){
				long array[] = { ttArray[i], ttArray[j] };
				if(array[0] > array[1]){
					long tmp = array[0];
					array[0] = array[1];
					array[1] = tmp;
				}
				context.write(_key, new HVArray(array));
			}
		}
	}
	
	@Override
	public void setup(Context context){
		ttList = new TLongArrayList();
	}
	
	@Override
	public void cleanup(Context context){
		ttList.clear();
		ttList = null;
	}
}

class ChordalSquareStageTwoDriver extends Configured implements Tool{
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// args: <edge file> <stageOneOutputDir> <outputDir> <numReducers> <jarFile>
		Configuration conf = getConf();
		int numReducers = Integer.parseInt(args[3]);

		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
		//conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
		
		Job job = new Job(conf, "TwinTwig ChordalSquare Stage Two");
		((JobConf)job.getConfiguration()).setJar(args[4]);
		
	    MultipleInputs.addInputPath(job, new Path(args[0]), 
	    		SequenceFileInputFormat.class, ChordalSquareEdgeMapper.class);
		
	    MultipleInputs.addInputPath(job, new Path(args[1]), 
	    		SequenceFileInputFormat.class, ChordalSquareStageTwoMapper.class);

	    //job.setMapperClass(SquareMapper.class);			
		job.setReducerClass(ChordalSquareStageTwoReducer.class);

		job.setMapOutputKeyClass(HVArraySign.class);
		job.setMapOutputValueClass(HVArray.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(HVArray.class);

		job.setSortComparatorClass(HVArraySignComparator.class);
		job.setGroupingComparatorClass(HVArrayGroupComparator.class);
		
		job.setNumReduceTasks(numReducers);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));	
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}

class ChordalSquareStageTwoMapper extends
		Mapper<HVArray, HVArray, HVArraySign, HVArray> {
	@Override
	public void map(HVArray key, HVArray value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArraySign(key, Config.LARGESIGN), value);
	}
}

class ChordalSquareEdgeMapper extends
		Mapper<LongWritable, LongWritable, HVArraySign, HVArray> {
	@Override
	public void map(LongWritable key, LongWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArraySign(key.get(), value.get(), Config.SMALLSIGN), new HVArray());
	}
}

class ChordalSquareStageTwoReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		for(HVArray value : values){
			if(_key.sign == Config.SMALLSIGN){
				continue;
			}
			long[] array = { value.getFirst(), _key.vertexArray.getFirst(), 
					value.getSecond(), _key.vertexArray.getSecond() };
			context.write(NullWritable.get(), new HVArray(array));
		}
	}
}

