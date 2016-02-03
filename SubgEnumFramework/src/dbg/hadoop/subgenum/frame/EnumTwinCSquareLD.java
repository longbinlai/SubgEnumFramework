package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
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

/**
 * Twin Chordal Square with non-optimal execution plan.
 * The execution plan is left deep.
 * @author robeen
 *
 */
public class EnumTwinCSquareLD {

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
		
		// Second round, join another triangle
		String[] opts = { workDir + "triangle.res", workDir + "frame.tcsquare.res.1",	
				workDir + "frame.tcsquare.res.2", inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new GeneralDriver(
				"Frame TwinChordalSquare Left-Deep",
				EnumTwinCSquareLDTriangleMapper.class,
				EnumTwinCSquareLDS2Mapper.class,
				EnumTwinCSquareLDS2Reducer.class,
				HVArray.class,
				HVArray.class, // OutputKV
				HVArraySign.class,
				HVArray.class, // MapOutputKV
				SequenceFileInputFormat.class,
				SequenceFileInputFormat.class,
				SequenceFileOutputFormat.class,
				HVArraySignComparator.class, HVArrayGroupComparator.class),
				opts);
		
		// Thrid round, final join another triangle
		String[] opts2 = { workDir + "triangle.res", workDir + "frame.tcsquare.res.2",	
				workDir + "frame.tcsquare.res", inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new GeneralDriver(
				"Frame TwinChordalSquare Left-Deep",
				EnumTwinCSquareLDTriangleMapper.class,
				EnumTwinCSquareLDS3Mapper.class,
				EnumTwinCSquareLDS3Reducer.class,
				NullWritable.class,
				LongWritable.class, // OutputKV
				HVArraySign.class,
				HVArray.class, // MapOutputKV
				SequenceFileInputFormat.class,
				SequenceFileInputFormat.class,
				SequenceFileOutputFormat.class,
				HVArraySignComparator.class, HVArrayGroupComparator.class),
				opts2);
	}
}

// Belows are stage two
/**
 * Let's deal with triangle
 * @author robeen
 *
 */
class EnumTwinCSquareLDTriangleMapper extends
Mapper<NullWritable, HVArray, HVArraySign, HVArray> {

	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long v1 = _value.getFirst();
		long v2 = _value.getSecond();
		long v3 = _value.getLast();
		context.write(new HVArraySign(v1, v2, Config.SMALLSIGN), new HVArray(v3));
		context.write(new HVArraySign(v2, v1, Config.SMALLSIGN), new HVArray(v3));
		context.write(new HVArraySign(v1, v3, Config.SMALLSIGN), new HVArray(v2));
		context.write(new HVArraySign(v3, v1, Config.SMALLSIGN), new HVArray(v2));
		context.write(new HVArraySign(v2, v3, Config.SMALLSIGN), new HVArray(v1));
		context.write(new HVArraySign(v3, v2, Config.SMALLSIGN), new HVArray(v1));
	}
}

class EnumTwinCSquareLDS2Mapper extends
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
				context.write(new HVArraySign(v3, v4, Config.LARGESIGN),  new HVArray(v1, v2));
				context.write(new HVArraySign(v3, v2, Config.LARGESIGN),  new HVArray(v1, v4));
				context.write(new HVArraySign(v1, v4, Config.LARGESIGN),  new HVArray(v3, v2));
				context.write(new HVArraySign(v1, v2, Config.LARGESIGN),  new HVArray(v3, v4));
			}
		}
	}
}

class EnumTwinCSquareLDS2Reducer extends
		Reducer<HVArraySign, HVArray, HVArray, HVArray> {

	private static ArrayList<HVArray> heap = new ArrayList<HVArray>();

	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		Iterator<HVArray> iter = null;
		HVArray temp = null;
		heap.clear();
		long v1, v2, v5;
		for (HVArray val : values) {
			if (_key.sign == Config.SMALLSIGN) {
				heap.add(new HVArray(val));
			} else {
				iter = heap.iterator();
				while (iter.hasNext()) {
					temp = iter.next();
					v1 = val.getFirst();
					v2 = val.getSecond();
					v5 = temp.getFirst();
					if (v1 < v5 && v2 != v5) {
						context.write(_key.vertexArray, new HVArray(v1, v2, v5));
					}
				}
			}
		}
	}
}

//Belows are stage three

class EnumTwinCSquareLDS3Mapper extends
		Mapper<HVArray, HVArray, HVArraySign, HVArray> {

	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArraySign(_key.getFirst(),  _value.getLast(), Config.LARGESIGN), 
				new HVArray(_value.getFirst(), _value.getSecond(), _key.getSecond()));
	}
}

class EnumTwinCSquareLDS3Reducer extends
		Reducer<HVArraySign, HVArray, NullWritable, LongWritable> {

	private static ArrayList<HVArray> heap = new ArrayList<HVArray>();

	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		Iterator<HVArray> iter = null;
		HVArray temp = null;
		heap.clear();
		long v1, v2, v4, v6;
		long count = 0;
		for (HVArray val : values) {
			if (_key.sign == Config.SMALLSIGN) {
				heap.add(new HVArray(val));
			} else {
				iter = heap.iterator();
				while (iter.hasNext()) {
					temp = iter.next();
					v1 = val.getFirst();
					v2 = val.getSecond();
					v4 = temp.getFirst();
					v6 = val.getLast();
					if (v1 != v4 && v2 != v4 && v6 != v4) {
						++count;
					}
				}
			}
		}
		context.write(NullWritable.get(), new LongWritable(count));
	}
}
