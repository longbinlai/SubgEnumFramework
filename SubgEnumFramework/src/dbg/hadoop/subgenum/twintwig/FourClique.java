package dbg.hadoop.subgenum.twintwig;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;

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

public class FourClique{
	
	public static void main(String[] args) throws Exception {
		run(new InputInfo(args));
	}
	
	public static void run(InputInfo inputInfo) throws Exception{
		String inputFilePath = inputInfo.inputFilePath;
		float falsePositive = inputInfo.falsePositive;
		boolean enableBF = inputInfo.enableBF;
		boolean isHyper = inputInfo.isHyper;
		
		String jarFile = inputInfo.jarFile;
		String numReducers = inputInfo.numReducers;
		String workDir = inputInfo.workDir;

		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		String stageOneOutput = workDir + "tt.4clique.tmp.1";
		String stageTwoOutput = workDir + "tt.4clique.res";
		
		Utility.getFS().delete(new Path(stageOneOutput));
		Utility.getFS().delete(new Path(stageTwoOutput));
		
		Configuration conf = new Configuration();
		conf.setBoolean("enable.bloom.filter", enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", (float)falsePositive);
		conf.setBoolean("count.only", inputInfo.isCountOnly);
		
		if(enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.EDGE + "." + falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
					.toString() + "/" + Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String adjListDir = isHyper ? workDir + Config.hyperGraphAdjList + ".0" :
				workDir + Config.adjListDir + ".0";
		
		// The parameters: <inputDir> <outputDir> <numReducers> <jarFile>
		String opts[] = {adjListDir, stageOneOutput, numReducers, jarFile};		
		ToolRunner.run(conf, new FourCliqueStageOneDriver(), opts);
		
		String opts2[] = {adjListDir, stageOneOutput, stageTwoOutput, numReducers, jarFile};
		ToolRunner.run(conf, new FourCliqueStageTwoDriver(), opts2);
		
		Utility.getFS().delete(new Path(stageOneOutput));
		//Utility.getFS().delete(new Path(stageTwoOutput));
	}
}


/**
 * The four clique enumeration, stage one, driver. 
 * @author robeen
 *
 */
class FourCliqueStageOneDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <inputDir> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "TwinTwig FourClique Stage One");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		job.setReducerClass(FourCliqueStageOneReducer.class);
		
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
				FourCliqueStageOneMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}

class FourCliqueStageOneMapper extends
		Mapper<LongWritable, HyperVertexAdjList, HVArraySign, HVArray> {

	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;
	private TwinTwigGenerator ttwigGen = null;

	// The hypervertex set
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context)
			throws IOException, InterruptedException {
		if (enableBF) {
			ttwigGen = new TwinTwigGenerator(key.get(), value,
						bloomfilterOpr.get());
		} else {
			ttwigGen = new TwinTwigGenerator(key.get(), value);
		}
		ttwigGen.genTwinTwigOne(context, Config.SMALLSIGN, (byte) 3, (byte) 5);
	}

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.
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

	@Override
	public void cleanup(Context context) {
		if (ttwigGen != null) {
			ttwigGen.clear();
			ttwigGen = null;
		}
	}
}

/**
 * Suppose the four clique is (v1, v2, v3, v4) as follows: <br>
 * 
 * v1----v4 <br>
 * | \ / | <br>
 * | / \ | <br>
 * v2----v3 <br>
 * 
 *  The key we receive here is <br>
 * (v2, v3). Using secondary sort, we let the twinTwig ((v2,v3); v1) comes before <br>
 * ((v2,v3); v4). We then first save v1 to the list, and then when we start to process <br>
 * v4, we check its availability. There are several constraints: <br>
 * 1. (v1, v2, v3, v4): Since each vertex is acturally a hyper vertex, which may duplicate in <br>
 * a match, but the number of duplications should not exceed the size of the hyper vertex; <br> 
 * 2. (v1, v2, v3, v4): Should follow the vertex order, which is v1 < v2 < v3 < v4; <br>;
 * 3. The BloomFilter tested: (v1, v4); (v2, v4) should pass the test. 
 * @author robeen
 *
 */
class FourCliqueStageOneReducer
	extends Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	
	private TLongArrayList list = null;
	private static boolean enableBF;
	private static BloomFilterOpr bloomfilterOpr;
	
	//private static boolean applyV1Filter;
	//private static int largeDegreeThresh = 0;
	
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {	
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		long v2 = _key.vertexArray.getFirst();
		long v4 = _key.vertexArray.getSecond();
		
		list.clear();
		for(HVArray value : values){
			if(_key.sign == Config.SMALLSIGN){
				list.add(value.getFirst()); // Add v1 to the list
			}
			else{
				TLongIterator iter = list.iterator();
				while(iter.hasNext()){
					long v1 = iter.next();
					long v3 = value.getFirst();
					long[] tmpArray = {v1, v2, v3, v4};
					boolean isOutput = Utility.isValidCand(tmpArray);
					//System.out.println(isOutput);
					if(isOutput){ // Check candidate validation
						if(enableBF){
							if(v1 != v3){
								if(!bloomfilterOpr.get().test(HyperVertex.VertexID(v1), 
										HyperVertex.VertexID(v3))){
									isOutput = false;
								}
							}
						}
						if(v1 == v3){
							isOutput = HyperVertex.isClique(v1);
						}
						if(isOutput){
							context.write(NullWritable.get(), new HVArray(tmpArray));
						}
					}
				}
			}
		}
	}
	
	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		list = new TLongArrayList();
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		//System.out.println(enableBF);
		if(enableBF && bloomfilterOpr == null){
			bloomfilterOpr = new BloomFilterOpr(conf.getFloat
					("bloom.filter.false.positive.rate", (float) 0.001));
			try {
				bloomfilterOpr.obtainBloomFilter(conf);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		}
	}
	
	@Override
	public void cleanup(Context context){
		list.clear();
		list = null;
	}
}

/**
 * The four clique enumeration, stage two, driver
 * @author robeen
 *
 */
class FourCliqueStageTwoDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <inputDir> <stageOneOutpuDir> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[3]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "TwinTwig FourClique Stage Two");
		((JobConf)job.getConfiguration()).setJar(args[4]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		boolean isCountOnly = conf.getBoolean("count.only", false);
		System.out.println(isCountOnly);
		
		job.setMapOutputKeyClass(HVArraySign.class);
		job.setMapOutputValueClass(HVArray.class);
		job.setOutputKeyClass(NullWritable.class);
		
		if(!isCountOnly) {
			job.setReducerClass(FourCliqueStageTwoReducer.class);
			job.setOutputValueClass(HVArray.class);
		}
		else {
			job.setReducerClass(FourCliqueStageTwoCountReducer.class);
			job.setOutputValueClass(LongWritable.class);
		}
		
		job.setSortComparatorClass(HVArraySignComparator.class);
		job.setGroupingComparatorClass(HVArrayGroupComparator.class);
		
		job.setNumReduceTasks(numReducers);
		
		MultipleInputs.addInputPath(job, 
				new Path(args[0]),
				SequenceFileInputFormat.class,
				FourCliqueStageTwoMapper1.class);
		
		MultipleInputs.addInputPath(job, 
				new Path(args[1]),
				SequenceFileInputFormat.class,
				FourCliqueStageTwoMapper2.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType
								(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}

class FourCliqueStageTwoMapper1 extends
		Mapper<LongWritable, HyperVertexAdjList, HVArraySign, HVArray> {

	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;

	private TwinTwigGenerator ttwigGen = null;

	// The hypervertex set
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context)
			throws IOException, InterruptedException {
		// if(HyperVertex.Degree(key.get()) < 3){
		// return;
		// }
		if (enableBF) {
			ttwigGen = new TwinTwigGenerator(key.get(), value,
					bloomfilterOpr.get());
		} else {
			ttwigGen = new TwinTwigGenerator(key.get(), value);
		}
		// keyMap = 7 = 111, v1, v2, v3 as the key;
		ttwigGen.genTwinTwigTwo(context, Config.SMALLSIGN, (byte) 7);
	}

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.
		enableBF = conf.getBoolean("enable.bloom.filter", true);
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

	@Override
	public void cleanup(Context context) {
		ttwigGen.clear();
		ttwigGen = null;
	}
}

class FourCliqueStageTwoMapper2 extends
		Mapper<NullWritable, HVArray, HVArraySign, HVArray> {

	// The hypervertex set
	@Override
	public void map(NullWritable key, HVArray value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArraySign(value.get(2), value.get(0), value.get(3),
				Config.LARGESIGN), new HVArray(value.get(1)));
	}
}

/**
 * Suppose the four clique is (v1, v2, v3, v4) as follows: <br>
 * 
 * v1----v4 <br>
 * | \ / | <br>
 * | / \ | <br>
 * v2----v3 <br>
 * 
 * We read v1, v2, v3, v4 from previous output, and then check the existence of v1, v3, v4
 * @author robeen
 *
 */
class FourCliqueStageTwoReducer
	extends Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {	
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		long v1 = _key.vertexArray.get(1);
		long v3 = _key.vertexArray.get(0);
		long v4 = _key.vertexArray.get(2);

		for(HVArray value : values){
			if(_key.sign == Config.SMALLSIGN){
				continue;
			}
			else{
				long array[] = {v1, value.getFirst(), v3, v4};
				context.write(NullWritable.get(), new HVArray(array));
			}
		}
	}
}


class FourCliqueStageTwoCountReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, LongWritable> {

	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		long count = 0L;
		for (HVArray value : values) {
			if (_key.sign == Config.SMALLSIGN) {
				continue;
			} else {
				++count;
			}
		}
		if(count != 0)
			context.write(NullWritable.get(), new LongWritable(count));
	}
}


