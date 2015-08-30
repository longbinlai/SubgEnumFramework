package dbg.hadoop.subgenum.frame;

import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.utils.BinarySearch;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class EnumSolarSquare{
	
	public static void run(InputInfo inputInfo) throws Exception{
		String workDir = inputInfo.workDir;
		if(!inputInfo.isChordalSquareSkip){
			EnumChordalSquare.run(inputInfo);
		}
		Configuration conf = new Configuration();
		conf.setBoolean("result.compression", inputInfo.isResultCompression);

		String[] opts = { workDir + "frame.csquare.res", "", workDir + "frame.solarsquare.res", 
				inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new GeneralDriver("Frame SolarSquare", 
				EnumSolarSquareMapper.class, 
				EnumSolarSquareReducer.class, 
			    HVArray.class, HVArray.class, //OutputKV
				HVArray.class, LongWritable.class, //MapOutputKV
				SequenceFileInputFormat.class, 
				SequenceFileOutputFormat.class,
				HVArrayComparator.class), opts);
	}
	
	public static void countOnce(InputInfo inputInfo) throws Exception{
		if(inputInfo.isCountPatternOnce && inputInfo.isResultCompression){
			String[] opts = { inputInfo.workDir + "frame.solarsquare.res",
					inputInfo.workDir + "frame.solarsquare.cnt", 
					inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					SolarSquareCountMapper.class), opts);
		}
	}
}

class EnumSolarSquareMapper extends
		Mapper<HVArray, HVArray, HVArray, LongWritable> {
	
	private boolean isCompress = true;
	
	@Override
	public void map(HVArray _key, HVArray _value,
			Context context) throws IOException, InterruptedException {
		if(!isCompress){
			context.write(new HVArray(_key.getFirst(), _value.getFirst(), _value.getSecond()),
					new LongWritable(_key.getSecond()));
			context.write(new HVArray(_key.getSecond(), _value.getFirst(), _value.getSecond()),
					new LongWritable(_key.getFirst()));
		} else {
			long[] array = _value.toArrays();
			for(int i = 0; i < array.length - 1; ++i){
				for(int j = i + 1; j < array.length; ++j){
					context.write(new HVArray(_key.getFirst(), array[i], array[j]),
							new LongWritable(_key.getSecond()));
					context.write(new HVArray(_key.getSecond(), array[i], array[j]),
							new LongWritable(_key.getFirst()));
				}
			}
		}
	}
	
	@Override
	public void setup(Context context){
		isCompress = context.getConfiguration().getBoolean("result.compression", true);
	}
}

class EnumSolarSquareReducer extends
	Reducer<HVArray, LongWritable, HVArray, HVArray> {
	
	private boolean isCompress = true;
	private HyperVertexHeap heap = null;
	
	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		heap.clear();
		for(LongWritable val: values){
			heap.insert(val.get());
		}
		heap.sort();
		long[] array = heap.toArrays();
		if(!isCompress){
			int largeThanMinIndex = BinarySearch.findLargeIndex(_key.getSecond(), array);
			for(int i = 0; i < largeThanMinIndex; ++i){
				for(int j = i + 1; j < array.length; ++j){
					context.write(_key, new HVArray(array[i], array[j]));
				}
			}
		}
		else{
			context.write(_key, new HVArray(array));		
		}
	}
	
	@Override
	public void setup(Context context){
		isCompress = context.getConfiguration().getBoolean("result.compression", true);
		heap = new HyperVertexHeap(Config.HEAPINITSIZE);
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
		context.write(NullWritable.get(), new LongWritable(count));
	}
}