package dbg.hadoop.subgenum.frame;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sun.awt.SunHints.Value;

import com.hadoop.compression.lzo.LzoCodec;
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import dbg.hadoop.subgenum.hypergraph.bloomfilter.GenBloomFilter;
import dbg.hadoop.subgenum.hypergraph.triangle.Triangle;
import dbg.hadoop.subgenum.prepare.PrepareData;
import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class EnumSquare {
	private static InputInfo inputInfo  = new InputInfo();

	public static void main(String[] args) throws Exception {
		inputInfo = new InputInfo(args);
		String workDir = inputInfo.workDir;
		long startTime=System.currentTimeMillis();   
			
		String[] opts0 = { workDir + "adjList2", workDir + "nonsmallneigh",inputInfo.numReducers, inputInfo.jarFile};
		ToolRunner.run(new Configuration(), new CalNonSmallDriver(), opts0);
		System.out.println("End of Calculate non small neighborhood nodes");
		
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + "/nonsmallneigh"), conf);
		
		conf.setBoolean("enable.bloom.filter", inputInfo.enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", inputInfo.falsePositive);
		if(inputInfo.enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.TWINTWIG1 + "." + inputInfo.falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + 
					Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String[] opts = { workDir + "adjList2", workDir + "frame.square",	inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(new Configuration(), new EnumSquareDriver(), opts);
		System.out.println("End of Enumeration");

		long endTime=System.currentTimeMillis();
		System.out.println(" " + (endTime - startTime) / 1000 + "s");
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
		Configuration conf = new Configuration();
		// The parameters: <inputfile> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
		Job job = new Job(conf, "EnumSquare");
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
	
	public void map(LongWritable _key, HyperVertexAdjList _value, Context context) throws IOException, InterruptedException{		

		long[] neighbors = _value.getNeighbors();
		TLongArrayList validNbrs = null;
		if(invalidNodeSet != null){
			validNbrs = new TLongArrayList();
			for(long v: neighbors){
				if(!invalidNodeSet.contains(v)){
					validNbrs.add(v);
				}
			}
		}
		boolean isOutput = true;
		for (int i = 0; i < validNbrs.size() - 1; ++i) {
			for (int j = i + 1; j < validNbrs.size(); ++j) {
				long v1 = validNbrs.get(i);
				long v2 = validNbrs.get(j);
				if(enableBF){
					isOutput = bloomfilterOpr.get().test(HyperVertex.VertexID(v1), 
							HyperVertex.VertexID(v2));
				}
				if(isOutput)
					context.write(new HVArray(v1, v2), _key);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);

		enableBF = conf.getBoolean("enable.bloom.filter", false);
		try {
			if (enableBF && bloomfilterOpr == null) {
				bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
						"bloom.filter.false.positive.rate", (float) 0.001));
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






