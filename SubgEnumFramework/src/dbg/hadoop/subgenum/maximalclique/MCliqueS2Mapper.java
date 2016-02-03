package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;

public class MCliqueS2Mapper
		extends Mapper<HVArray, HVArray, LongWritable, HVArray> {
	@Override
	public void map(HVArray key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		for(long u : value.toArrays()){
			context.write(new LongWritable(u), key);
		}
	}
}