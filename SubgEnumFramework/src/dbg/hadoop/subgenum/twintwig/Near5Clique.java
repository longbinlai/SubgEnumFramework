package dbg.hadoop.subgenum.twintwig;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgenum.frame.GeneralDriver;
import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayGroupComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.TwinTwigGenerator;
import dbg.hadoop.subgraphs.utils.Utility;

@SuppressWarnings("deprecation")
public class Near5Clique{
	
	public static void main(String[] args) throws Exception {
		run(new InputInfo(args));
	}
	
	public static void run(InputInfo inputInfo) throws Exception{
		String inputFilePath = inputInfo.inputFilePath;
		float falsePositive = inputInfo.falsePositive;
		boolean enableBF = inputInfo.enableBF;
		boolean isCountOnly = inputInfo.isCountOnly;
		
		String jarFile = inputInfo.jarFile;
		String numReducers = inputInfo.numReducers;
		String workDir = inputInfo.workDir;

		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		String output = workDir + "tt.near5clique.res";
		
		if(!inputInfo.isFourCliqueSkip){
			inputInfo.isCountOnly = false;
			FourClique.run(inputInfo);
		}
		inputInfo.isCountOnly = isCountOnly;
		
		Configuration conf = new Configuration();
		conf.setBoolean("enable.bloom.filter", enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", (float)falsePositive);
		
		if(enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.EDGE + "." + falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
					.toString() + "/" + Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String adjListDir = workDir + Config.adjListDir + "." + inputInfo.maxSize;
		String stageOneOutput = workDir + "tt.4clique.res";
		String stageTwoOutput = workDir + "tt.near5clique.res";
		
		if(Utility.getFS().isDirectory(new Path(stageTwoOutput))) {
			Utility.getFS().delete(new Path(stageTwoOutput));
		}
		String[] opts = { adjListDir, stageOneOutput, stageTwoOutput,
				numReducers, jarFile };
		
		if (!isCountOnly) {
			ToolRunner.run(conf, new GeneralDriver(
					"TwinTwig Near5Clique",
					Near5CliqueMapper1.class,
					Near5CliqueMapper2.class,
					Near5CliqueReducer.class,
					NullWritable.class,
					HVArray.class, // OutputKV
					HVArraySign.class,
					HVArray.class, // MapOutputKV
					SequenceFileInputFormat.class,
					SequenceFileInputFormat.class,
					SequenceFileOutputFormat.class,
					HVArraySignComparator.class, HVArrayGroupComparator.class),
					opts);
		} else {
			ToolRunner.run(conf, new GeneralDriver(
					"TwinTwig Near5Clique",
					Near5CliqueMapper1.class,
					Near5CliqueMapper2.class,
					Near5CliqueCountReducer.class,
					NullWritable.class,
					LongWritable.class, // OutputKV
					HVArraySign.class,
					HVArray.class, // MapOutputKV
					SequenceFileInputFormat.class,
					SequenceFileInputFormat.class,
					SequenceFileOutputFormat.class,
					HVArraySignComparator.class, HVArrayGroupComparator.class),
					opts);
		}
		
		Utility.getFS().delete(new Path(stageOneOutput));
		Utility.getFS().delete(new Path(stageTwoOutput));
		
	}
}

class Near5CliqueMapper1 extends 
	Mapper<LongWritable, HyperVertexAdjList, HVArraySign, HVArray> {
	
	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;

	private TwinTwigGenerator ttwigGen = null;

	// The hypervertex set
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context)
			throws IOException, InterruptedException {
		if (enableBF) {
			ttwigGen = new TwinTwigGenerator(key.get(), value, bloomfilterOpr.get());
		} else {
			ttwigGen = new TwinTwigGenerator(key.get(), value);
		}
		ttwigGen.genTwinTwigOne(context, Config.SMALLSIGN, (byte) 3, (byte) 0);
		ttwigGen.genTwinTwigTwo(context, Config.SMALLSIGN, (byte) 3);
		ttwigGen.genTwinTwigThree(context, Config.SMALLSIGN, (byte) 3);
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

class Near5CliqueMapper2 extends 
	Mapper<NullWritable, HVArray, HVArraySign, HVArray> {
	
	@Override
	public void map(NullWritable _key, HVArray _value, Context context) 
			throws IOException, InterruptedException {
		long[] array = _value.toArrays();
		context.write(new HVArraySign(array[0], array[1], Config.LARGESIGN), 
				new HVArray(array[2], array[3]));
			context.write(new HVArraySign(array[0], array[2], Config.LARGESIGN), 
				new HVArray(array[1], array[3]));
			context.write(new HVArraySign(array[0], array[3], Config.LARGESIGN), 
				new HVArray(array[1], array[2]));
			context.write(new HVArraySign(array[1], array[2], Config.LARGESIGN), 
				new HVArray(array[0], array[3]));
			context.write(new HVArraySign(array[1], array[3], Config.LARGESIGN), 
				new HVArray(array[0], array[2]));
			context.write(new HVArraySign(array[2], array[3], Config.LARGESIGN), 
				new HVArray(array[0], array[1]));
	}
}

class Near5CliqueReducer extends
	Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	
	private static TLongArrayList ttList = null;
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {	
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		ttList.clear();
		long v2 = _key.vertexArray.getFirst();
		long v5 = _key.vertexArray.getSecond();
		for(HVArray value : values){
			if(_key.sign == Config.SMALLSIGN)
				ttList.add(value.getFirst());
			else {
				TLongIterator iter = ttList.iterator();
				while(iter.hasNext()){
					long v1 = iter.next();
					long v3 = value.getFirst();
					long v4 = value.getSecond();
					if(v1 != v3 && v1 != v4){
						long[] out = { v1, v2, v3, v4, v5 };
						context.write(NullWritable.get(), new HVArray(out));
					}
				}
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

class Near5CliqueCountReducer extends
	Reducer<HVArraySign, HVArray, NullWritable, LongWritable> {
private static TLongHashSet triSet = null;
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {	
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		triSet.clear();
		long count = 0L;
		long v3 = 0, v4 = 0;
		//long v2 = _key.vertexArray.getFirst();
		//long v5 = _key.vertexArray.getSecond();
		for(HVArray value : values){
			if(_key.sign == Config.SMALLSIGN)
				//ttList.add(value.getFirst());
				triSet.add(value.getFirst());
			else {
				count += triSet.size();
				v3 = value.getFirst();
				v4 = value.getSecond();
				if(triSet.contains(v3)) {
					count -= 1;
				}
				if(triSet.contains(v4)) {
					count -= 1;
				}
			}
		}
		context.write(NullWritable.get(), new LongWritable(count));
	}
	
	@Override
	public void setup(Context context){
		triSet = new TLongHashSet();
	}
	
	@Override
	public void cleanup(Context context){
		triSet.clear();
		triSet = null;
	}
}
