package dbg.hadoop.subgenum.frame;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BinarySearch;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

public class EnumSquare {
	private static InputInfo inputInfo  = new InputInfo();

	public static void main(String[] args) throws Exception {
		inputInfo = new InputInfo(args);
		String workDir = inputInfo.workDir;
		int maxSize = inputInfo.maxSize;
		
		if (workDir.toLowerCase().contains("hdfs")) {
			int pos = workDir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(workDir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		if(Utility.getFS().isDirectory(new Path(workDir + "frame.square.res"))){
			Utility.getFS().delete(new Path(workDir + "frame.square.res"));
		}
		if(Utility.getFS().isDirectory(new Path(workDir + "frame.square.cnt"))){
			Utility.getFS().delete(new Path(workDir + "frame.square.cnt"));
		}
		
		long startTime=System.currentTimeMillis();   
		
		if(!Utility.getFS().isDirectory(new Path(workDir + "nonsmallneigh"))){
			String[] opts0 = { workDir + "adjList2.0", workDir + "nonsmallneigh",inputInfo.numReducers, inputInfo.jarFile};
			ToolRunner.run(new Configuration(), new CalNonSmallDriver(), opts0);
			System.out.println("End of Calculate non small neighborhood nodes");
		}
		
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + "/nonsmallneigh"), conf);
		
		conf.setInt("mapred.input.max.size", maxSize);
		conf.setBoolean("enable.bloom.filter", inputInfo.enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", inputInfo.falsePositive);
		if(inputInfo.enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.TWINTWIG1 + "." + inputInfo.falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + "/" +
					Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String[] opts = { workDir + "adjList2." + maxSize, workDir + "frame.square.res",	inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new EnumSquareDriver(), opts);
		System.out.println("End of Enumeration");

		long endTime=System.currentTimeMillis();
		System.out.println(" " + (endTime - startTime) / 1000 + "s");
		
		if (inputInfo.isCountPatternOnce) {
			String[] opts2 = { workDir + "frame.square.res", workDir + "frame.square.cnt", 
					inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(conf, new GeneralPatternCountDriver(SquareCountMapper.class), opts2);
		}
	}
	
}

class CalNonSmallDriver extends Configured implements Tool{
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		// The parameters: <inputfile> <outputDir> <numReducers> <seperator> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		Job job = new Job(conf, "CalNonSmall");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		job.setMapperClass(CalNonSmallMapper.class);
		job.setReducerClass(CalNonSmallReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}

class CalNonSmallMapper extends
		Mapper<LongWritable, HyperVertexAdjList, NullWritable, LongWritable> {
	public void map(LongWritable _key, HyperVertexAdjList _value,
			Context context) throws IOException, InterruptedException {
		if (_value.getSmallNum() == 0)
			context.write(NullWritable.get(), _key);

	}
}

class CalNonSmallReducer extends
		Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
	@Override
	public void reduce(NullWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		for (LongWritable val : values) {
			context.write(NullWritable.get(), val);
		}
	}
}

class EnumSquareDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <inputfile> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
		Job job = new Job(conf, "Frame Square");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		job.setMapperClass(EnumSquareMapper.class);
		job.setReducerClass(EnumSquareReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		job.setMapOutputKeyClass(HVArray.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(HVArray.class);
		job.setOutputValueClass(HVArray.class);
		job.setSortComparatorClass(HVArrayComparator.class);
		
		job.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}

class EnumSquareMapper extends Mapper<LongWritable, HyperVertexAdjList, HVArray, LongWritable> {
	private static TLongHashSet invalidNodeSet = null;
	private static boolean enableBF = false;
	private static BloomFilterOpr bloomfilterOpr = null;
	private static int maxSize = 0;
	
	public void map(LongWritable _key, HyperVertexAdjList _value, Context context) throws IOException, InterruptedException{
		if (maxSize == 0) {
			long[] neighbors = _value.getNeighbors();
			TLongArrayList validNbrs = new TLongArrayList();
			for (long v : neighbors) {
				if (!invalidNodeSet.contains(v)) {
						validNbrs.add(v);
				}
			}
			handleOutput(validNbrs.toArray(), null, _key, context);
		}
		else{
			TLongArrayList validNbrs1 = new TLongArrayList();
			TLongArrayList validNbrs2 = new TLongArrayList();
			if(!_value.existBackup()){
				for (long v : _value.getSmallDegreeVerticesGroup1()) {
					if (!invalidNodeSet.contains(v)) {
						validNbrs1.add(v);
					}
				}
				for (long v : _value.getLargeDegreeVertices()) {
					if (!invalidNodeSet.contains(v)) {
						validNbrs2.add(v);
					}
				}
			} else {
				for (long v : _value.getSmallDegreeVerticesGroup0()) {
					if (!invalidNodeSet.contains(v)) {
						validNbrs1.add(v);
					}
				}
				for (long v : _value.getSmallDegreeVerticesGroup1()) {
					if (!invalidNodeSet.contains(v)) {
						validNbrs2.add(v);
					}
				}
			}
			if(!_value.existBackup()){
				if(_value.isFirstAdd()){
					handleOutput(validNbrs2.toArray(), null, _key, context);
				}
				handleOutput(validNbrs1.toArray(), null, _key, context);
			}
			handleOutput(validNbrs1.toArray(), validNbrs2.toArray(), _key, context);
		}
	}
	
	private void handleOutput(long[] array1, long[] array2, LongWritable _key, Context context) 
			throws IOException, InterruptedException{
		boolean isOutput = true;
		if(array2 == null){
			for(int i = 0; i < array1.length - 1; ++i){
				for(int j = i + 1; j < array1.length; ++j){
					long v1 = array1[i];
					long v2 = array1[j];
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
								HyperVertex.VertexID(v1),
								HyperVertex.VertexID(v2));
					}
					if(isOutput){
						context.write(new HVArray(v1, v2), _key);
					}
				}
			}
		}
		else{
			for(int i = 0; i < array1.length; ++i){
				for(int j = 0; j < array2.length; ++j){
					long v1 = array1[i];
					long v2 = array2[j];
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
								HyperVertex.VertexID(v1),
								HyperVertex.VertexID(v2));
					}
					if(isOutput){
						context.write(new HVArray(v1, v2), _key);
					}
				}
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		maxSize = conf.getInt("mapred.input.max.size", 0);
		FileSystem fs = FileSystem.get(conf);
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		try {
			if (enableBF && bloomfilterOpr == null) {
				bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
						"bloom.filter.false.positive.rate", (float) 0.001), Config.TWINTWIG1);
				bloomfilterOpr.obtainBloomFilter(conf);
			}

			// Read the invalid nodes
			if (invalidNodeSet == null) {
				invalidNodeSet = new TLongHashSet();
				for (int i = 0; i < paths.length; ++i) {
					if (paths[i].toString().contains("part-r-")) {
						SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[i], conf);
						NullWritable key = null;
						LongWritable val = null;
						while (reader.next(key, val)) {
							invalidNodeSet.add(val.get());
						}
						reader.close();
					}
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}

class EnumSquareReducer extends	Reducer<HVArray,LongWritable, HVArray, HVArray> {
	private static HyperVertexHeap heap = null;
	
	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException{
		heap.clear();
		for (LongWritable val : values) {
			heap.insert(val.get());
		}
		heap.sort();
		long[] vertices = heap.toArrays();
		context.write(_key, new HVArray(vertices));
	}
	
	@Override
	public void setup(Context context){
		heap = new HyperVertexHeap(Config.HEAPINITSIZE);
	}
	
	@Override
	public void cleanup(Context context){
		heap.clear();
		heap = null;
	}
}

class SquareCountMapper extends
		Mapper<HVArray, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long count = 0L;
		int largeThanMinIndex = BinarySearch.findLargeIndex(_key.getFirst(), _value.toArrays());
		count = (2 * _value.size() - 1 - largeThanMinIndex) * largeThanMinIndex / 2;
		context.write(NullWritable.get(), new LongWritable(count));
	}
}





