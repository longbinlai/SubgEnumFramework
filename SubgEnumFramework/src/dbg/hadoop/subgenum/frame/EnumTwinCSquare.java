package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayGroupComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

public class EnumTwinCSquare {

	public static void run(InputInfo inputInfo) throws Exception {
		String workDir = Utility.getWorkDir(inputInfo.inputFilePath);
		Configuration conf = new Configuration();
		// Make sure the configuration is correct
		inputInfo.enableBF = false;
		inputInfo.isCountOnly = false;
		inputInfo.isResultCompression = true;
		// First round, generate chordal square left
		inputInfo.outputDir = "frame.tcsquare.res.1";
		EnumChordalSquare.run(inputInfo);
		// Second round, generate chordal square right
		inputInfo.outputDir = "frame.tcsquare.res.2";
		EnumChordalSquare.run(inputInfo);
		// Thrid round, join the two chordal squares
		String[] opts = { workDir + "frame.tcsquare.res.1", workDir + "frame.tcsquare.res.2",	
				workDir + "frame.tcsquare.res", inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new GeneralDriver(
				"Frame TwinChordalSquare",
				EnumTwinCSquareMapper1.class,
				EnumTwinCSquareMapper2.class,
				EnumTwinCSquareReducer.class,
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
	
	public static void countOnce(InputInfo inputInfo) throws Exception{
		if (inputInfo.isCountPatternOnce) {
			String[] opts = { inputInfo.workDir + "frame.tcsquare.res",
					inputInfo.workDir + "frame.tcsquare.cnt",
					inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					GeneralPatternCountIdentityMapper.class), opts);
		}
	}

}

class EnumTwinCSquareMapper1 extends
Mapper<HVArray, HVArray, HVArraySign, HVArray> {

	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long v1 = _key.getFirst();
		long v3 = _key.getLast();
		long v2 = 0, v4 = 0;
		long[] array = _value.toArrays();
		for(int i = 0; i < array.length - 1; ++i){
			v2 = array[i];
			for(int j = i + 1; j < array.length; ++j){
				v4 = array[j];
				context.write(new HVArraySign(v3, v4, Config.SMALLSIGN),  new HVArray(v1, v2));
				context.write(new HVArraySign(v3, v2, Config.SMALLSIGN),  new HVArray(v1, v4));
				context.write(new HVArraySign(v1, v4, Config.SMALLSIGN),  new HVArray(v3, v2));
				context.write(new HVArraySign(v1, v2, Config.SMALLSIGN),  new HVArray(v3, v4));
			}
		}
	}
}

class EnumTwinCSquareMapper2 extends
Mapper<HVArray, HVArray, HVArraySign, HVArray> {

	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long v1 = _key.getFirst();
		long v3 = _key.getLast();
		long v2 = 0, v4 = 0;
		long[] array = _value.toArrays();
		for(int i = 0; i < array.length - 1; ++i){
			v2 = array[i];
			for(int j = i + 1; j < array.length; ++j){
				v4 = array[j];
				context.write(new HVArraySign(v1, v2, Config.LARGESIGN),  new HVArray(v4, v3));
				context.write(new HVArraySign(v1, v4, Config.LARGESIGN),  new HVArray(v2, v3));
				context.write(new HVArraySign(v3, v2, Config.LARGESIGN),  new HVArray(v4, v1));
				context.write(new HVArraySign(v3, v4, Config.LARGESIGN),  new HVArray(v2, v1));
			}
		}
	}
}

class EnumTwinCSquareReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, LongWritable> {

	private static ArrayList<HVArray> heap = new ArrayList<HVArray>();

	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		long count = 0;
		Iterator<HVArray> iter = null;
		HVArray temp = null;
		heap.clear();
		long v1, v2, v4, v5;
		for(HVArray val : values){
			if(_key.sign == Config.SMALLSIGN){
				heap.add(new HVArray(val));
			}
			else{
				iter = heap.iterator();
				while(iter.hasNext()){
					temp = iter.next();
					v1 = temp.getFirst();
					v2 = temp.getSecond();
					v4 = val.getFirst();
					v5 = val.getSecond();
					if(v1 < v5 && v1 != v4 && v2 != v4 && v2 != v5){
						++count;
					}
				}
			}
		}
		context.write(NullWritable.get(), new LongWritable(count));
	}
}