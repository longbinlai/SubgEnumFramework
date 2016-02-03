package dbg.hadoop.subgenum.frame;

import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.utils.BinarySearch;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;

@SuppressWarnings("deprecation")
public class EnumSolarSquare{
	
	public static void run(InputInfo inputInfo) throws Exception{
		String workDir = inputInfo.workDir;
		
		Configuration conf = new Configuration();
		conf.setBoolean("result.compression", inputInfo.isResultCompression);
		conf.setBoolean("enable.bloom.filter", inputInfo.enableBF);
		conf.setBoolean("enum.solarsquare.chordalsquare.partition", inputInfo.isChordalSquarePartition);
		boolean isCountOnly = inputInfo.isCountOnly;

		if(inputInfo.enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.TWINTWIG1 + "." + inputInfo.falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + "/" +
					Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		if(inputInfo.isChordalSquarePartition && inputInfo.isResultCompression){
			GeneralPartitioner.run(workDir + "frame.csquare.res", inputInfo.chordalSquarePartitionThresh, 
					inputInfo.numReducers, inputInfo.jarFile);
		}
		
		if(!inputInfo.isChordalSquareSkip){
			inputInfo.isCountOnly = false;
			EnumChordalSquare.run(inputInfo);
		}
		inputInfo.isCountOnly = isCountOnly;

		String[] opts = { workDir + "frame.csquare.res", "", workDir + "frame.solarsquare.res", 
				inputInfo.numReducers, inputInfo.jarFile };
		if(inputInfo.isChordalSquarePartition && inputInfo.isResultCompression){
			opts[0] = workDir + "frame.csquare.res.part";
		}
		if(!isCountOnly) {
			ToolRunner.run(conf, new GeneralDriver("Frame SolarSquare", 
				EnumSolarSquareMapper.class, 
				EnumSolarSquareReducer.class, 
			    HVArray.class, HVArray.class, //OutputKV
				HVArray.class, LongWritable.class, //MapOutputKV
				SequenceFileInputFormat.class, 
				SequenceFileOutputFormat.class,
				HVArrayComparator.class), opts);
		}
		else {
			ToolRunner.run(conf, new GeneralDriver("Frame SolarSquare", 
					EnumSolarSquareMapper.class, 
					EnumSolarSquareCountReducer.class, 
				    NullWritable.class, LongWritable.class, //OutputKV
					HVArray.class, LongWritable.class, //MapOutputKV
					SequenceFileInputFormat.class, 
					SequenceFileOutputFormat.class,
					HVArrayComparator.class), opts);
		}
	}
	
	public static void countOnce(InputInfo inputInfo) throws Exception{
		if(inputInfo.isCountPatternOnce){
			String[] opts = { inputInfo.workDir + "frame.solarsquare.res",
					inputInfo.workDir + "frame.solarsquare.cnt", 
					inputInfo.numReducers, inputInfo.jarFile };
			if(inputInfo.isCountOnly) {
				ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
						GeneralPatternCountIdentityMapper.class), opts);
			}
			else if(inputInfo.isResultCompression) {
				ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					SolarSquareCountMapper.class), opts);
			}
			else {
				System.out.println("Not count needed");
			}
		}
	}
}


class EnumSolarSquareMapper extends
		Mapper<HVArray, HVArray, HVArray, LongWritable> {
	
	private boolean isCompress = true;
	private boolean isPart = false;
	private static boolean enableBF = false;
	private static BloomFilterOpr bloomfilterOpr = null;
	
	@Override
	public void map(HVArray _key, HVArray _value,
			Context context) throws IOException, InterruptedException {
		if(!isCompress){
			context.write(new HVArray(_key.getFirst(), _value.getFirst(), _value.getSecond()),
				new LongWritable(_key.getSecond()));
			context.write(new HVArray(_key.getSecond(), _value.getFirst(), _value.getSecond()),
				new LongWritable(_key.getFirst()));
		} else {
			if(!isPart)
				this.processCompressChordalSquare(_key, _value.toArrays(), context);
			else
				this.processCompressChordalSquarePart(_key, _value.toArrays(), context);
		}
	}
	
	private void processCompressChordalSquare(HVArray _key, long[] array, Context context) 
			throws IOException, InterruptedException{
		//long[] array = _value.toArrays();
		boolean isOutput = true;
		for(int i = 0; i < array.length - 1; ++i){
			for(int j = i + 1; j < array.length; ++j){
				if(enableBF){
					isOutput = bloomfilterOpr.get().test(HyperVertex.VertexID(array[i]), 
							HyperVertex.VertexID(array[j]));
				}
				if(isOutput){
					context.write(new HVArray(_key.getFirst(), array[i], array[j]),
						new LongWritable(_key.getSecond()));
					context.write(new HVArray(_key.getSecond(), array[i], array[j]),
						new LongWritable(_key.getFirst()));
				}
			}
		}
	}
	
	private void processCompressChordalSquarePart(HVArray _key, long[] array, Context context) 
			throws IOException, InterruptedException{
		boolean isOutput = true;
		if (array[0] == -1) {
			this.processCompressChordalSquare(_key,
					Arrays.copyOfRange(array, 1, array.length), context);
		} else {
			int len = (int)array[0];
			for(int i = 1; i < len + 1; ++i){
				for(int j = len + 1; j < array.length; ++j){
					if(enableBF){
						isOutput = bloomfilterOpr.get().test(HyperVertex.VertexID(array[i]), 
								HyperVertex.VertexID(array[j]));
					}
					if(isOutput){
						context.write(new HVArray(_key.getFirst(), array[i], array[j]),
							new LongWritable(_key.getSecond()));
						context.write(new HVArray(_key.getSecond(), array[i], array[j]),
							new LongWritable(_key.getFirst()));
					}
				}
			}
		}
	}
	
	
	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		isCompress = conf.getBoolean("result.compression", true);
		isPart = conf.getBoolean("enum.solarsquare.chordalsquare.partition", false);
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		try {
			if (enableBF && isCompress && bloomfilterOpr == null) {
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
}

class EnumSolarSquareReducer extends
	Reducer<HVArray, LongWritable, HVArray, HVArray> {
	
	private boolean isCompress = true;
	private TLongArrayList heap = null;
	
	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		heap.clear();
		for(LongWritable val: values){
			heap.add(val.get());
		}
		heap.sort();
		long[] array = heap.toArray();
		if (!isCompress) {
			int largeThanMinIndex = BinarySearch.findLargeIndex(_key.getSecond(), array);
			for (int i = 0; i < largeThanMinIndex; ++i) {
				for (int j = i + 1; j < array.length; ++j) {
					context.write(_key, new HVArray(array[i], array[j]));
				}
			}
		} else {
			context.write(_key, new HVArray(array));
		}
	}

	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		isCompress = conf.getBoolean("result.compression", true);
		heap = new TLongArrayList();
	}
}

class EnumSolarSquareCountReducer extends
		Reducer<HVArray, LongWritable, NullWritable, LongWritable> {
	
	private boolean isCompress = true;
	private TLongArrayList heap = null;

	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		heap.clear();
		for (LongWritable val : values) {
			heap.add(val.get());
		}
		heap.sort();
		
		long count = 0L;
		long[] array = heap.toArray();
		
		int largeThanMinIndex = BinarySearch.findLargeIndex(
					_key.getSecond(), array);
		if(isCompress) {
			count = (2 * heap.size() - 1 - largeThanMinIndex) * largeThanMinIndex / 2;
		} 
		else { // Not compressed, touch every result once
			for (int i = 0; i < largeThanMinIndex; ++i) {
				for (int j = i + 1; j < array.length; ++j) {
					count += 1;
				}
			}
		}
		if(count > 0)
			context.write(NullWritable.get(), new LongWritable(count));
	}

	@Override
	public void setup(Context context) {
		heap = new TLongArrayList();
		isCompress = context.getConfiguration().getBoolean("result.compression", true);
	}
}

class SolarSquareCountMapper extends
		Mapper<HVArray, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long count = 0L;
		int largeThanMinIndex = BinarySearch.findLargeIndex(_key.getSecond(), _value.toArrays());
		count = (2 * _value.size() - 1 - largeThanMinIndex) * largeThanMinIndex / 2;
		if(count > 0)
			context.write(NullWritable.get(), new LongWritable(count));
	}
}