package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;

public class MCliqueS3Mapper
		extends Mapper<LongWritable, HVArray, LongWritable, LongWritable> {
	@Override
	public void map(LongWritable key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		for(long v : value.toArrays()){
			context.write(new LongWritable(v), key);
		}
	}
}