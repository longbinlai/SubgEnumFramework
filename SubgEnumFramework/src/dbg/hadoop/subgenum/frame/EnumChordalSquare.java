package dbg.hadoop.subgenum.frame;

import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;
import java.net.URI;

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
		
		String[] opts = { workDir + "triangle.res", "", workDir + "frame.csquare.res",
				inputInfo.numReducers, inputInfo.jarFile};
		
		if (!inputInfo.isCountOnly) {
			ToolRunner.run(conf, new GeneralDriver("Frame ChordalSquare",
					EnumChordalSquareMapper.class,
					EnumChordalSquareReducer.class,
					HVArray.class,
					HVArray.class, // OutputKV
					HVArray.class,
					LongWritable.class, // MapOutputKV
					SequenceFileInputFormat.class,
					SequenceFileOutputFormat.class, HVArrayComparator.class),
					opts);
		} else {
			ToolRunner.run(conf, new GeneralDriver("Frame ChordalSquare",
					EnumChordalSquareMapper.class,
					EnumChordalSquareCountReducer.class,
					NullWritable.class,
					LongWritable.class, // OutputKV
					HVArray.class,
					LongWritable.class, // MapOutputKV
					SequenceFileInputFormat.class,
					SequenceFileOutputFormat.class, HVArrayComparator.class),
					opts);
		}
	}
	
	public static void countOnce(InputInfo inputInfo) throws Exception{
		if (inputInfo.isCountPatternOnce) {
			String[] opts2 = { inputInfo.workDir + "frame.csquare.res",
					inputInfo.workDir + "frame.csquare.cnt",
					inputInfo.numReducers, inputInfo.jarFile };
			if(inputInfo.isCountOnly) {
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
					//long[] out = { array[i], _key.getFirst(), array[j], _key.getSecond() };
					// Bloomfilter only used in the enumeration of solar square
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

	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long count = 0L;
		for (LongWritable val : values) {
			count += 1;
		}
		if(count > 1)
			context.write(NullWritable.get(), new LongWritable(count * (count - 1) / 2));
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