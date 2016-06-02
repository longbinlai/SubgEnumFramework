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
import org.apache.hadoop.mapreduce.Mapper.Context;
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

public class SixClique{
	
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
		
		String stageOneOutput = workDir + "tt.6clique.tmp.1";
		String stageTwoOutput = workDir + "tt.6clique.tmp.2";
		String stageThreeOutput = workDir + "tt.6clique.tmp.3";
		String stageFourOutput = workDir + "tt.6clique.tmp.4";
		String stageFiveOutput = workDir + "tt.6clique.tmp.5";
		String stageSixOutput = workDir + "tt.6clique.tmp.6";
		String stageSevenOutput = workDir + "tt.6clique.res";
		
		Utility.getFS().delete(new Path(stageOneOutput));
		Utility.getFS().delete(new Path(stageTwoOutput));
		Utility.getFS().delete(new Path(stageThreeOutput));
		Utility.getFS().delete(new Path(stageFourOutput));
		Utility.getFS().delete(new Path(stageFiveOutput));
		Utility.getFS().delete(new Path(stageSixOutput));
		Utility.getFS().delete(new Path(stageSevenOutput));
		
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
		
		// The parameters: <inputDir> <outputDir> <numReducers> <jarFile>
		// Stage 1 and 2, generate four cliques
		String opts[] = {adjListDir, stageOneOutput, numReducers, jarFile};		
		ToolRunner.run(conf, new FourCliqueStageOneDriver(), opts);
		
		String opts2[] = {adjListDir, stageOneOutput, stageTwoOutput, numReducers, jarFile};
		ToolRunner.run(conf, new FourCliqueStageTwoDriver(), opts2);
		
		// Stage 3 and 4, generate five cliques
		conf.setInt("twin.twig.output.key.map", 3);
		String opts3[] = {adjListDir, stageTwoOutput, stageThreeOutput, numReducers, jarFile};
		ToolRunner.run(conf, new FiveCliqueStageThreeDriver(), opts3);
		
		conf.setInt("twin.twig.output.key.map", 7);
		//conf.setBoolean("count.only", inputInfo.isCountOnly);
		String opts4[] = {adjListDir, stageThreeOutput, stageFourOutput, numReducers, jarFile};
		ToolRunner.run(conf, new FiveCliqueStageFourDriver(), opts4);
		
		// Stage 5 - 7, generate six cliques
		
		conf.setInt("twin.twig.output.key.map", 3);
		String opts5[] = {adjListDir, stageFourOutput, stageFiveOutput, numReducers, jarFile};
		ToolRunner.run(conf, new SixCliqueStageFiveDriver(), opts5);
		
		conf.setInt("twin.twig.output.key.map", 7);
		String opts6[] = {adjListDir, stageFiveOutput, stageSixOutput, numReducers, jarFile};
		ToolRunner.run(conf, new SixCliqueStageSixDriver(), opts6);
		
		conf.setBoolean("count.only", inputInfo.isCountOnly);
		String opts7[] = {edgeDir, stageSixOutput, stageSevenOutput, numReducers, jarFile};
		ToolRunner.run(conf, new SixCliqueStageSevenDriver(), opts7);
		
		Utility.getFS().delete(new Path(stageOneOutput));
		Utility.getFS().delete(new Path(stageTwoOutput));
		Utility.getFS().delete(new Path(stageThreeOutput));
		Utility.getFS().delete(new Path(stageFourOutput));
		Utility.getFS().delete(new Path(stageFiveOutput));
		Utility.getFS().delete(new Path(stageSixOutput));
		
	}
}

/**
 * The six clique enumeration, stage five, driver. 
 * @author robeen
 *
 */
class SixCliqueStageFiveDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <adjListDir> <stageTwoOutput> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[3]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "TwinTwig SixClique Stage Five");
		((JobConf)job.getConfiguration()).setJar(args[4]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		job.setReducerClass(SixCliqueStageFiveReducer.class);
		
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
				SixCliqueTwinTwigMapper.class);
		
		MultipleInputs.addInputPath(job, 
				new Path(args[1]),
				SequenceFileInputFormat.class,
				SixCliqueStageFiveMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}

class SixCliqueTwinTwigMapper extends
		Mapper<LongWritable, HyperVertexAdjList, HVArraySign, HVArray> {

	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;
	private static byte ttwigOutputKeyMap = (byte)3;
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
		ttwigGen.genTwinTwigOne(context, Config.SMALLSIGN, ttwigOutputKeyMap, (byte) 0);
	}

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		ttwigOutputKeyMap = (byte) conf.getInt("twin.twig.output.key.map", 3);
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

class SixCliqueStageFiveMapper extends
		Mapper<NullWritable, HVArray, HVArraySign, HVArray> {
	
	@Override
	public void map(NullWritable key, HVArray value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArraySign(value.get(0), value.get(1), Config.LARGESIGN), 
				new HVArray(value.get(2), value.get(3), value.get(4)));
	}
}

class SixCliqueStageFiveReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	private static TLongArrayList ttOneList = null;
	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;
	
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		ttOneList.clear();
		
		long v1 = _key.vertexArray.get(0);
		long v2 = _key.vertexArray.get(1);
		
		boolean isOutput = true;

		for (HVArray value : values) {
			if (_key.sign == Config.SMALLSIGN) {
				ttOneList.add(value.getFirst());
			} else {
				for (long v0 : ttOneList.toArray()) {
					if(enableBF){
						isOutput = bloomfilterOpr.get().test(HyperVertex.VertexID(v0), 
								HyperVertex.VertexID(value.get(0))) && 
								bloomfilterOpr.get().test(HyperVertex.VertexID(v0), 
										HyperVertex.VertexID(value.get(1))) &&
								bloomfilterOpr.get().test(HyperVertex.VertexID(v0), 
										HyperVertex.VertexID(value.get(2)));
					}
					if(isOutput){
						long array[] = { v0, v1, v2, value.get(0), value.get(1), value.get(2) };
						//System.out.println(HyperVertex.HVArrayToString(array));
						context.write(NullWritable.get(), new HVArray(array));
					}
				}
			}
		}
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
		ttOneList = new TLongArrayList();
	}
	
	@Override
	public void cleanup(Context context){
		ttOneList.clear();
		ttOneList = null;
	}
}

class SixCliqueStageSixDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <adjListDir> <stageThreeOutput> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[3]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "TwinTwig SixClique Stage Six");
		((JobConf)job.getConfiguration()).setJar(args[4]);
		//JobConf job = new JobConf(getConf(), this.getClass());

		job.setMapOutputKeyClass(HVArraySign.class);
		job.setMapOutputValueClass(HVArray.class);
		job.setOutputKeyClass(NullWritable.class);
		
		job.setReducerClass(SixCliqueStageSixReducer.class);
		job.setOutputValueClass(HVArray.class);

		
		job.setSortComparatorClass(HVArraySignComparator.class);
		job.setGroupingComparatorClass(HVArrayGroupComparator.class);
		
		job.setNumReduceTasks(numReducers);
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		MultipleInputs.addInputPath(job, 
				new Path(args[0]),
				SequenceFileInputFormat.class,
				SixCliqueTwinTwigMapper.class);
		
		MultipleInputs.addInputPath(job, 
				new Path(args[1]),
				SequenceFileInputFormat.class,
				SixCliqueStageSixMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}

class SixCliqueStageSixMapper extends
		Mapper<NullWritable, HVArray, HVArraySign, HVArray> {
	@Override
	public void map(NullWritable key, HVArray value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArraySign(value.get(0), value.get(3), value.get(4),
				Config.LARGESIGN), new HVArray(value.get(1), value.get(2), value.get(5)));
	}
}

class SixCliqueStageSixReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		
		long v0 = _key.vertexArray.get(0);
		long v3 = _key.vertexArray.get(1);
		long v4 = _key.vertexArray.get(2);

		for (HVArray value : values) {
			if (_key.sign == Config.SMALLSIGN) {
				continue;
			} else {
				long[] array = {v0, value.get(0), value.get(1), v3, v4, value.get(2)};
				//System.out.println(HyperVertex.HVArrayToString(array));
				context.write(NullWritable.get(), new HVArray(array));
			}
		}
	}
}

class SixCliqueStageSevenDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <edgeDir> <stageThreeOutput> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[3]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "TwinTwig SixClique Stage seven");
		((JobConf)job.getConfiguration()).setJar(args[4]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		boolean isCountOnly = conf.getBoolean("count.only", false);
		
		job.setMapOutputKeyClass(HVArraySign.class);
		job.setMapOutputValueClass(HVArray.class);
		job.setOutputKeyClass(NullWritable.class);
		
		if(!isCountOnly) {
			job.setReducerClass(SixCliqueStageSevenReducer.class);
			job.setOutputValueClass(HVArray.class);
		}
		else{
			job.setReducerClass(SixCliqueCountReducer.class);
			job.setOutputValueClass(LongWritable.class);
		}
		
		job.setSortComparatorClass(HVArraySignComparator.class);
		job.setGroupingComparatorClass(HVArrayGroupComparator.class);
		
		job.setNumReduceTasks(numReducers);
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		MultipleInputs.addInputPath(job, 
				new Path(args[0]),
				SequenceFileInputFormat.class,
				SixCliqueEdgeMapper.class);
		
		MultipleInputs.addInputPath(job, 
				new Path(args[1]),
				SequenceFileInputFormat.class,
				SixCliqueStageSevenMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.waitForCompletion(true);
		return 0;
	}
}

class SixCliqueEdgeMapper extends
		Mapper<LongWritable, LongWritable, HVArraySign, HVArray> {
	@Override
	public void map(LongWritable key, LongWritable value, Context context)
			throws IOException, InterruptedException {

		context.write(new HVArraySign(key.get(), value.get(),
				Config.SMALLSIGN), new HVArray());
	}
}


class SixCliqueStageSevenMapper extends
		Mapper<NullWritable, HVArray, HVArraySign, HVArray> {
	@Override
	public void map(NullWritable key, HVArray value, Context context)
			throws IOException, InterruptedException {
		long[] array = {value.get(1), value.get(2), value.get(3), value.get(4)};
		context.write(new HVArraySign(value.get(0), value.get(5), Config.LARGESIGN), 
				new HVArray(array));
	}
}

class SixCliqueStageSevenReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, HVArray> {

	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}

		long v0 = _key.vertexArray.get(0);
		long v5 = _key.vertexArray.get(1);

		for (HVArray value : values) {
			if (_key.sign == Config.SMALLSIGN) {
				continue;
			} else {
				long[] array = { v0, value.get(0), value.get(1), value.get(2), value.get(3), v5 };
				//System.out.println(HyperVertex.HVArrayToString(array));
				context.write(NullWritable.get(), new HVArray(array));
			}
		}
	}
}

class SixCliqueCountReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, LongWritable> {

	@Override
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
		if(count != 0) {
			context.write(NullWritable.get(), new LongWritable(count));
		}
	}
}
