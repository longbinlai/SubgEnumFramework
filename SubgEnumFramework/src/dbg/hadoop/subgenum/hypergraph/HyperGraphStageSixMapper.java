package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;


public class HyperGraphStageSixMapper
		extends Mapper<LongWritable, LongWritable, HVArray, NullWritable> {
	// The hypervertex set
	@Override
	public void map(LongWritable key, LongWritable value, Context context) 
			throws IOException, InterruptedException{
		context.write(new HVArray(key.get(), value.get()), NullWritable.get());
	}
}

