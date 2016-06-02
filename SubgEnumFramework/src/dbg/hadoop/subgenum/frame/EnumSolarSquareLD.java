package dbg.hadoop.subgenum.frame;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HVArrayGroupComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.InputInfo;

@SuppressWarnings("deprecation")
public class EnumSolarSquareLD{
	
	public static void run(InputInfo inputInfo) throws Exception{
		String workDir = inputInfo.workDir;
		
		Configuration conf = new Configuration();
		//conf.setBoolean("result.compression", inputInfo.isResultCompression);

		inputInfo.isResultCompression = false;
		inputInfo.isCountOnly = false;
		inputInfo.outputDir = "frame.solarsquare.res.1";
		EnumChordalSquare.run(inputInfo);
		
		// Second stage, join the triangle to the chordal square
		String[] opts = { workDir + "triangle.res", workDir + "frame.solarsquare.res.1",	
				workDir + "frame.solarsquare.res.2", inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new GeneralDriver(
				"Frame SolarSquare Left-Deep S2",
				EnumSolarSquareS2TriangleMapper.class,
				EnumSolarSquareS2CSquareMapper.class,
				EnumSolarSquareS2Reducer.class,
				NullWritable.class,
				HVArray.class, // OutputKV
				HVArraySign.class,
				HVArray.class, // MapOutputKV
				SequenceFileInputFormat.class,
				SequenceFileInputFormat.class,
				SequenceFileOutputFormat.class,
				HVArraySignComparator.class, HVArrayGroupComparator.class),
				opts);
		
		// Third stage
		opts[0] = workDir + Config.preparedFileDir;
		opts[1] = workDir + "frame.solarsquare.res.2";
		opts[2] = workDir + "frame.solarsquare.res";
		
		ToolRunner.run(conf, new GeneralDriver(
				"Frame SolarSquare Left-Deep S3",
				EnumSolarSquareS3EdgeMapper.class,
				EnumSolarSquareS3Mapper.class,
				EnumSolarSquareS3Reducer.class,
				NullWritable.class,
				LongWritable.class, // OutputKV
				HVArray.class,
				HVArray.class, // MapOutputKV
				SequenceFileInputFormat.class,
				SequenceFileInputFormat.class,
				SequenceFileOutputFormat.class,
				HVArrayComparator.class, null),
				opts);	
	}

}


/**
 * Let's deal with triangle
 * @author robeen
 *
 */
class EnumSolarSquareS2TriangleMapper extends
	Mapper<NullWritable, HVArray, HVArraySign, HVArray> {

	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long v1 = _value.getFirst();
		long v2 = _value.getSecond();
		long v3 = _value.getLast();
		context.write(new HVArraySign(v1, v3, Config.SMALLSIGN), new HVArray(v2));
		context.write(new HVArraySign(v2, v3, Config.SMALLSIGN), new HVArray(v1));
		context.write(new HVArraySign(v3, v2, Config.SMALLSIGN), new HVArray(v1));
	}
}

class EnumSolarSquareS2CSquareMapper extends
	Mapper<HVArray, HVArray, HVArraySign, HVArray>{
	
	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long v1 = _key.getFirst();
		long v2 = _value.getFirst();
		long v3 = _key.getSecond();
		long v4 = _value.getSecond();
		
		context.write(new HVArraySign(v1, v2, Config.LARGESIGN), new HVArray(v3, v4));
		context.write(new HVArraySign(v3, v2, Config.LARGESIGN), new HVArray(v1, v4));
	}
}

class EnumSolarSquareS2Reducer extends
	Reducer<HVArraySign, HVArray, NullWritable, HVArray>{
	
	private static TLongArrayList list = new TLongArrayList();
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values, Context context)
			throws IOException, InterruptedException{
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		long v0 = _key.vertexArray.getFirst();
		long v2 = _key.vertexArray.getSecond();
		long v1 = 0, v3 = 0, v4 = 0;
		TLongIterator iter = null;
		list.clear();
		for(HVArray val : values){
			if(_key.sign == Config.SMALLSIGN){
				list.add(val.getFirst());
			}
			else{
				iter = list.iterator();
				while(iter.hasNext()){
					v1 = iter.next();
					v3 = val.getFirst();
					v4 = val.getSecond();
					
					if(v1 < v3 && v1 < v4){
						long[] temp = {v0, v1, v2, v3, v4};
						context.write(NullWritable.get(), new HVArray(temp));
					}
				}
			}
		}
	}
}


class EnumSolarSquareS3Mapper extends
	Mapper<NullWritable, HVArray, HVArray, HVArray> {

	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long v0 = _value.getFirst();
		long v1 = _value.getSecond();
		long v2 = _value.get(2);
		long v3 = _value.get(3);
		long v4 = _value.get(4);
		context.write(new HVArray(v1, v4), new HVArray(v0, v2, v3));
	}
}

class EnumSolarSquareS3EdgeMapper extends
		Mapper<LongWritable, LongWritable, HVArray, HVArray> {
	
	private static final HVArray minusOne = new HVArray(-1L);
	@Override
	public void map(LongWritable _key, LongWritable _value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArray(_key.get(), _value.get()), minusOne);
	}
}

class EnumSolarSquareS3Reducer extends
		Reducer<HVArray, HVArray, NullWritable, LongWritable> {

	@Override
	public void reduce(HVArray _key, Iterable<HVArray> values, Context context)
			throws IOException, InterruptedException {
		long count = 0L;
		boolean isOut = false;
		for(HVArray val : values){
			if(val.size() == 1){
				isOut = true;
			}
			else{
				++count;
			}
		}
		if(isOut && count != 0){
			context.write(NullWritable.get(), new LongWritable(count));
		}
	}
}