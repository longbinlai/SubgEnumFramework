package dbg.hadoop.subgenum.star;

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
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.TwinTwigGenerator;
import dbg.hadoop.subgraphs.utils.Utility;

public class Clique{
	
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
		int cliqueSize = Integer.valueOf(inputInfo.cliqueNumVertices);

		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		Configuration conf = new Configuration();
		conf.setBoolean("enable.bloom.filter", enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", (float)falsePositive);
		
		
		if(enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.EDGE + "." + falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
					.toString() + "/" + Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String adjListDir = isHyper ? workDir + Config.hyperGraphAdjList + ".0" :
				workDir + Config.adjListDir + ".0";
		
		String edgeDir = workDir + Config.preparedFileDir;	
		
		conf.setInt("partial.match.size", 6);
		conf.setInt("clique.number.vertices", cliqueSize);
		conf.setBoolean("count.only", false);
		
		String prevStageDir = adjListDir;
		String curStageDir = "";
		String prefix = "star." + cliqueSize + "clique.";
		
		
		for(int i = 1; i <= cliqueSize - 2; ++i){
			if(i == cliqueSize - 2){
				conf.setBoolean("count.only", inputInfo.isCountOnly);
			}
			if(i == 1) {
				conf.setBoolean("two.stars", true);
				conf.setInt("star.size.large", cliqueSize);
			}
			else {
				conf.setBoolean("two.stars", false);
				prevStageDir = workDir + prefix + (i - 1);
			}
			curStageDir = workDir + prefix + i;
			
			conf.setInt("star.size.small", cliqueSize - i);
			conf.setInt("value.size", i);
			conf.setInt("key.map", (1 << (cliqueSize - i)) - 1);
			conf.setInt("which.stage", i);
			String opts[] = {adjListDir, prevStageDir, curStageDir, numReducers, jarFile};
			
			// In case that the directory has existed
			Utility.getFS().delete(new Path(curStageDir));
			// Run the driver
			ToolRunner.run(conf, new CliqueDriver(), opts);
			// Clean up the partial results
			if(i > 1){
				Utility.getFS().delete(new Path(prevStageDir));
			}
		}
	}
}

/**
 * This is a template driver
 * @author robeen
 *
 */
class CliqueDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <inputDir> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[3]);
		int curStage = conf.getInt("which.stage", 1);
		boolean isCountOnly = conf.getBoolean("count.only", false);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "Star Clique stage " + curStage);
		((JobConf)job.getConfiguration()).setJar(args[4]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		if(isCountOnly)
			job.setReducerClass(CliqueCountReducer.class);	
		else
			job.setReducerClass(CliqueReducer.class);
		
		job.setMapOutputKeyClass(HVArraySign.class);
		job.setMapOutputValueClass(HVArray.class);
		job.setOutputKeyClass(NullWritable.class);
		if(isCountOnly)
			job.setOutputValueClass(LongWritable.class);
		else
			job.setOutputValueClass(HVArray.class);
		
		job.setSortComparatorClass(HVArraySignComparator.class);
		job.setGroupingComparatorClass(HVArrayGroupComparator.class);
		
		job.setNumReduceTasks(numReducers);
		
		if(curStage == 1)
			MultipleInputs.addInputPath(job, 
				new Path(args[0]),
				SequenceFileInputFormat.class,
				CliqueStarMapper.class);
		else {
			MultipleInputs.addInputPath(job, 
					new Path(args[0]),
					SequenceFileInputFormat.class,
					CliqueStarMapper.class);
			
			MultipleInputs.addInputPath(job, 
					new Path(args[1]),
					SequenceFileInputFormat.class,
					CliquePartialMapper.class);
		}

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}


/**
 * This is a template mapper to generate stars
 * @author robeen
 *
 */
class CliqueStarMapper extends
		Mapper<LongWritable, HyperVertexAdjList, HVArraySign, HVArray> {

	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;
	private static int starSizeSmall = 0;
	private static int starSizeLarge = 0;
	private static int keyMap = 0;
	private static boolean twoStars = false;

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
		ttwigGen.genStars(context, starSizeSmall, Config.SMALLSIGN, keyMap);
		if(twoStars)
			ttwigGen.genStars(context, starSizeLarge, Config.LARGESIGN, keyMap);
	}

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.
		enableBF = conf.getBoolean("enable.bloom.filter", true);
		twoStars = conf.getBoolean("two.stars", false);
		starSizeSmall = conf.getInt("star.size.small", 3);
		keyMap = conf.getInt("key.map", 7);
		if(twoStars){
			starSizeLarge = conf.getInt("star.size.large", 3);
		}
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

/**
 * This is a template mapper to handle partial matches
 * @author robeen
 *
 */
class CliquePartialMapper extends
		Mapper<NullWritable, HVArray, HVArraySign, HVArray> {
	
	private static int partialMatchSize = 0;
	private static int keyMap = 0;

	@Override
	public void map(NullWritable key, HVArray value, Context context)
			throws IOException, InterruptedException {
		long[] partial = value.toArrays();
		long[][] kv = Utility.getKeyValuePair(partial, keyMap);
		context.write(new HVArraySign(new HVArray(kv[0]), Config.LARGESIGN), new HVArray(kv[1]));
	}
	
	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		partialMatchSize = conf.getInt("partial.match.size", 3);
		keyMap = conf.getInt("key.map", 7);
	}
}

/**
 * This is template reducer
 * @author robeen
 *
 */
class CliqueReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	
	private static int valueSize = 0;
	private static int cliqueSize = 4;
	

	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		
		long[] array = new long[cliqueSize];
		for (HVArray val : values) {
			if (_key.sign == Config.SMALLSIGN) {
				continue;
			} else {
				System.arraycopy(val.toArrays(), 0, array, 0, valueSize);
				System.arraycopy(_key.vertexArray.toArrays(), 0, array, valueSize, cliqueSize - valueSize);
				
				System.out.println(HyperVertex.HVArrayToString(array));
				context.write(NullWritable.get(), new HVArray(array));
			}
		}
	}
	
	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		valueSize = conf.getInt("value.size", 3);
		cliqueSize = conf.getInt("clique.number.vertices", 4);
	}
}

/**
 * This is template reducer
 * @author robeen
 *
 */
class CliqueCountReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, LongWritable> {
	
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		
		long count = 0;
		for (HVArray val : values) {
			if (_key.sign == Config.SMALLSIGN) continue;
			else  ++count;
		}
		if(count != 0){
			context.write(NullWritable.get(), new LongWritable(count));
			//System.out.println(count);
		}
	}
}
