package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.utils.HyperVertex;

public class MCliqueS3DegreeMapper
		extends Mapper<IntWritable, IntWritable, LongWritable, LongWritable> {
	//private static final LongWritable zero = new LongWritable(0);
	@Override
	public void map(IntWritable key, IntWritable value, Context context) 
			throws IOException, InterruptedException{
		LongWritable v = new LongWritable(HyperVertex.get(key.get(), value.get()));
		context.write(v, v);
	}
}