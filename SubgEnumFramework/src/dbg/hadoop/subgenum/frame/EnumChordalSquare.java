package dbg.hadoop.subgenum.frame;

import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HVArrayGroupComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class EnumChordalSquare {

	public static void run(InputInfo inputInfo) throws Exception {
		//inputInfo = new InputInfo(args);
		String workDir = inputInfo.workDir;
		
		Configuration conf = new Configuration();
		conf.setBoolean("result.compression", inputInfo.isResultCompression);
		conf.setBoolean("enable.bloom.filter", inputInfo.enableBF);
		if(inputInfo.enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.TWINTWIG1 + "." + inputInfo.falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + "/" +
					Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String outputDir = inputInfo.outputDir == null ? workDir + "frame.csquare.res" : workDir + inputInfo.outputDir;
		
		if (!inputInfo.isNonOverlapping) {
			String[] opts = { workDir + "triangle.res", "", outputDir,
					inputInfo.numReducers, inputInfo.jarFile};
			if (!inputInfo.isCountOnly) {
				ToolRunner.run(conf,
						new GeneralDriver(
								"Frame ChordalSquare",
								EnumChordalSquareMapper.class,
								EnumChordalSquareReducer.class,
								HVArray.class,
								HVArray.class, // OutputKV
								HVArray.class,
								LongWritable.class, // MapOutputKV
								SequenceFileInputFormat.class,
								SequenceFileOutputFormat.class,
								HVArrayComparator.class), opts);
			} else {
				ToolRunner.run(conf,
						new GeneralDriver(
								"Frame ChordalSquare",
								EnumChordalSquareMapper.class,
								EnumChordalSquareCountReducer.class,
								NullWritable.class,
								LongWritable.class, // OutputKV
								HVArray.class,
								LongWritable.class, // MapOutputKV
								SequenceFileInputFormat.class,
								SequenceFileOutputFormat.class,
								HVArrayComparator.class), opts);
			}
		}
		else { // Non-overlapping case
			String[] opts = { workDir + "triangle.res", workDir + Config.adjListDir + "." + inputInfo.maxSize,	
					workDir + "frame.csquare.res", inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(conf, new GeneralDriver(
					"Frame Chordal Square NonOverlap",
					EnumCSNOPTriMapper.class,
					EnumCSNOPTTMapper.class,
					EnumCSNOPReducer.class,
					NullWritable.class,
					LongWritable.class, // OutputKV
					HVArraySign.class,
					LongWritable.class, // MapOutputKV
					SequenceFileInputFormat.class,
					SequenceFileInputFormat.class,
					SequenceFileOutputFormat.class,
					HVArraySignComparator.class, HVArrayGroupComparator.class),
					opts);
		}
	}
	
	public static void countOnce(InputInfo inputInfo) throws Exception{
		if (inputInfo.isCountPatternOnce) {
			String[] opts2 = { inputInfo.workDir + "frame.csquare.res",
					inputInfo.workDir + "frame.csquare.cnt",
					inputInfo.numReducers, inputInfo.jarFile };
			if(inputInfo.isCountOnly || inputInfo.isNonOverlapping) {
				ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
						GeneralPatternCountIdentityMapper.class), opts2);
			}
			else if(inputInfo.isResultCompression) {
				ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					ChordalSquareCountMapper.class), opts2);
			}
			else {
				System.out.println("Not count needed");
			}
		}
	}
}

class EnumChordalSquareMapper extends
	Mapper<NullWritable, HVArray, HVArray, LongWritable> {
	
	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArray(_value.getSecond(), _value.getLast()),
				new LongWritable(_value.getFirst()));
		context.write(new HVArray(_value.getFirst(), _value.getLast()),
				new LongWritable(_value.getSecond()));
		context.write(new HVArray(_value.getFirst(), _value.getSecond()),
				new LongWritable(_value.getLast()));
	}
}

class EnumChordalSquareReducer extends	
	Reducer<HVArray,LongWritable, HVArray, HVArray> {
	
	private static TLongArrayList heap = null;
	private static boolean isResultCompression = true;
	private static boolean enableBF = false;
	private static BloomFilterOpr bloomfilterOpr = null;
	
	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		heap.clear();
		for (LongWritable val : values) {
			heap.add(val.get());
		}  
		heap.sort();
		long[] array = heap.toArray();
		boolean isOutput = true;
		if(isResultCompression) {
			if(array.length > 1)
				context.write(_key, new HVArray(array));
		}
		else{
			for(int i = 0; i < array.length - 1; ++i){
				for(int j = i + 1; j < array.length; ++j){
					if(enableBF){
						isOutput = bloomfilterOpr.get().test(HyperVertex.VertexID(array[i]), 
								HyperVertex.VertexID(array[j]));
					}
					if(isOutput)
						context.write(_key, new HVArray(array[i], array[j]));
				}
			}
		}
	}
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		heap = new TLongArrayList();
		isResultCompression = conf.getBoolean(
				"result.compression", true);
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		try {
			if (enableBF && bloomfilterOpr == null) {
				bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
						"bloom.filter.false.positive.rate", (float) 0.001), Config.TWINTWIG1);
				bloomfilterOpr.obtainBloomFilter(conf);
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	@Override
	public void cleanup(Context context){
		heap = null;
	}
} 

class EnumChordalSquareCountReducer extends
		Reducer<HVArray, LongWritable, NullWritable, LongWritable> {
	
	private static TLongArrayList heap = null;
	private static boolean isResultCompression = true;

	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		heap.clear();
		long count = 0L;
		int len = 0;
		for (LongWritable val : values) {
			if(!isResultCompression)
				heap.add(val.get());
			++len;
		}
		if(isResultCompression) {
			if(len > 1)
				count += len * (len - 1) / 2;
		}
		else{
			heap.sort();
			long[] array = heap.toArray();
			for(int i = 0; i < array.length - 1; ++i){
				for(int j = i + 1; j < array.length; ++j){
					++count;
				}
			}
		}
		if(count > 0) {
			context.write(NullWritable.get(), new LongWritable(count));
		}
	}
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		heap = new TLongArrayList();
		isResultCompression = conf.getBoolean("result.compression", true);
	}

}


class ChordalSquareCountMapper extends
		Mapper<HVArray, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long size = _value.size();
		if(size > 1)
			context.write(NullWritable.get(), new LongWritable(size * (size - 1) / 2));
	}
}

/**
 * A mapper handling triangle part
 * @author robeen
 *
 */
class EnumCSNOPTriMapper extends
		Mapper<NullWritable, HVArray, HVArraySign, LongWritable> {

	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArraySign(_value.getSecond(), _value.getLast(),
				Config.SMALLSIGN), new LongWritable(_value.getFirst()));
		context.write(new HVArraySign(_value.getFirst(), _value.getLast(),
				Config.SMALLSIGN), new LongWritable(_value.getSecond()));
		context.write(new HVArraySign(_value.getFirst(), _value.getSecond(),
				Config.SMALLSIGN), new LongWritable(_value.getLast()));
	}
}

/**
 * A mapper handling twintwig part
 * @author robeen
 *
 */
class EnumCSNOPTTMapper extends
	Mapper<LongWritable, HyperVertexAdjList, HVArraySign, LongWritable> {


	@Override
	public void map(LongWritable _key, HyperVertexAdjList _value,
			Context context) throws IOException, InterruptedException {
		
		long[] largerThanThis = _value.getLargeDegreeVertices();
		long[] smallerThanThisG0 = _value.getSmallDegreeVerticesGroup0();
		long[] smallerThanThisG1 = _value.getSmallDegreeVerticesGroup1();


		if (_value.isFirstAdd()) {
			// Generate TwinTwig 1
			for (int i = 0; i < largerThanThis.length - 1; ++i) {
				for (int j = i + 1; j < largerThanThis.length; ++j) {
					context.write(new HVArraySign(largerThanThis[i],
							largerThanThis[j], Config.LARGESIGN), _key);
				}
			}
		}
		
		if (smallerThanThisG0.length == 0) {
			for (int i = 0; i < smallerThanThisG1.length; ++i) {
				// Generate TwinTwig 3
				for (int k = i + 1; k < smallerThanThisG1.length; ++k) {
					context.write(new HVArraySign(smallerThanThisG1[i],
								smallerThanThisG1[k], Config.LARGESIGN), _key);
				}
				// Generate TwinTwig 2
				for (int j = 0; j < largerThanThis.length; ++j) {
					context.write(new HVArraySign(smallerThanThisG1[i],
								largerThanThis[j], Config.LARGESIGN), _key);
				}
			}
		}

		else {
			for (int i = 0; i < smallerThanThisG0.length; ++i) {
				for (int j = 0; j < smallerThanThisG1.length; ++j) {
					context.write(new HVArraySign(smallerThanThisG0[i],
							smallerThanThisG1[j], Config.LARGESIGN), _key);
				}
			}
		}
	}
}

class EnumCSNOPReducer extends
		Reducer<HVArraySign, LongWritable, NullWritable, LongWritable> {

	private static ArrayList<Long> heap = new ArrayList<Long>();

	@Override
	public void reduce(HVArraySign _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		Iterator<Long> iter = null;
		heap.clear();
		long v2;
		long count = 0;
		for (LongWritable val : values) {
			if (_key.sign == Config.SMALLSIGN) {
				heap.add(val.get());
			} else {
				iter = heap.iterator();
				while (iter.hasNext()) {
					v2 = iter.next();
					if (v2 < val.get()) {
						++count;
					}
				}
			}
		}
		context.write(NullWritable.get(), new LongWritable(count));
	}
}