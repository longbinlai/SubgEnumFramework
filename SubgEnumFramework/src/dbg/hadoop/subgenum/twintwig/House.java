package dbg.hadoop.subgenum.twintwig;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import gnu.trove.list.array.TLongArrayList;

import dbg.hadoop.subgraphs.io.HVArray;
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


public class House{
	
	public static void main(String[] args) throws Exception {
		run(new InputInfo(args));
	}
	
	public static void run(InputInfo inputInfo) throws Exception{
		String numReducers = inputInfo.numReducers;
		String inputFilePath = inputInfo.inputFilePath;
		String jarFile = inputInfo.jarFile;
		boolean enableBloomFilter = inputInfo.enableBF;
		float bfProbFP = inputInfo.falsePositive;
		int maxSize = inputInfo.maxSize;
		String workDir = inputInfo.workDir;
		
		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		String stageOneOutput = workDir + "tt.house.tmp.1";
		String stageTwoOutput = workDir + "tt.house.res";
		
		Utility.getFS().delete(new Path(stageOneOutput));
		Utility.getFS().delete(new Path(stageTwoOutput));
		
		Configuration conf = new Configuration();
		if(enableBloomFilter){
			conf.setBoolean("enable.bloom.filter", true);
			conf.setDouble("bloom.filter.false.positive.rate", bfProbFP);
			//String bloomFilterFileName = "bloomFilter." + Config.TWINTWIG1 + "." + bfProbFP;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() 
					+ "/" + Config.bloomFilterFileDir + "/" + "bloomFilter." 
					+ Config.TWINTWIG1 + "." + bfProbFP), conf);
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() 
					+ "/" + Config.bloomFilterFileDir + "/" + "bloomFilter." 
					+ Config.EDGE + "." + bfProbFP), conf);
		}
		
		String[] opts1 = { workDir + Config.adjListDir + "." + maxSize, stageOneOutput, numReducers, jarFile };
		ToolRunner.run(conf, new SquareDriver(), opts1);
		
		String[] opts2 = { workDir + Config.adjListDir + "." + maxSize, stageOneOutput, stageTwoOutput, numReducers, jarFile };
		ToolRunner.run(conf, new HouseStageTwoDriver(), opts2);

		Utility.getFS().delete(new Path(stageOneOutput));
		Utility.getFS().delete(new Path(stageTwoOutput));
	}
}

/**
 * The house enumeration, stage two, driver. 
 * @author robeen
 *
 */
class HouseStageTwoDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <adjListDir> <stageOneOutput> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[3]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "TwinTwig House Stage Two");
		((JobConf)job.getConfiguration()).setJar(args[4]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		job.setReducerClass(HouseStageTwoReducer.class);
		
		job.setMapOutputKeyClass(HVArraySign.class);
		job.setMapOutputValueClass(HVArray.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(HVArray.class);
		
		job.setSortComparatorClass(HVArraySignComparator.class);
		job.setGroupingComparatorClass(HVArrayGroupComparator.class);
		
		job.setNumReduceTasks(numReducers);
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		MultipleInputs.addInputPath(job, 
				new Path(args[0]),
				SequenceFileInputFormat.class,
				HouseTwinTwigMapper.class);
		
		MultipleInputs.addInputPath(job, 
				new Path(args[1]),
				SequenceFileInputFormat.class,
				HouseStageTwoMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}

class HouseTwinTwigMapper extends
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

class HouseStageTwoMapper extends
		Mapper<NullWritable, HVArray, HVArraySign, HVArray> {
	@Override
	public void map(NullWritable key, HVArray value, Context context)
			throws IOException, InterruptedException {
		long v1 = value.get(0);
		long v2 = value.get(1);
		long v3 = value.get(2);
		long v4 = value.get(3);
		
		context.write(new HVArraySign(v1, v2, Config.LARGESIGN), new HVArray(v3, v4));
		context.write(new HVArraySign(v1, v4, Config.LARGESIGN), new HVArray(v2, v3));
		if(v2 < v3){
			context.write(new HVArraySign(v2, v3, Config.LARGESIGN), new HVArray(v1, v4));
		} else {
			context.write(new HVArraySign(v3, v2, Config.LARGESIGN), new HVArray(v1, v4));
		}
		if(v3 < v4){
			context.write(new HVArraySign(v3, v4, Config.LARGESIGN), new HVArray(v1, v2));
		} else {
			context.write(new HVArraySign(v4, v3, Config.LARGESIGN), new HVArray(v1, v2));
		}
	}
}

class HouseStageTwoReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	private static TLongArrayList ttList = null;
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		long v2 = _key.vertexArray.getFirst();
		long v5 = _key.vertexArray.getSecond();
		ttList.clear();
		for (HVArray value : values) {
			if (_key.sign == Config.SMALLSIGN) {
				ttList.add(value.getFirst());
			} else {
				for (long v1 : ttList.toArray()) {
					long v3 = value.getFirst();
					long v4 = value.getSecond();
					if(v1 != v3 && v1 != v4){
						long array[] = { v1, v2, v3, v4, v5 };
						context.write(NullWritable.get(), new HVArray(array));
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

