package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MCliqueS4Mapper
		extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
	@Override
	public void map(LongWritable key, LongWritable value, Context context) 
			throws IOException, InterruptedException{
		context.write(value, key);
	}
}