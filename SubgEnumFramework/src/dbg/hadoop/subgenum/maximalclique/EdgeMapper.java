package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.utils.Config;

/**
 * Get the statics of every node, specifically the degree
 * @author robeen
 *
 */
// public class InitMapper extends MapReduceBase implements
public class EdgeMapper
		extends Mapper<LongWritable, LongWritable, HVArraySign, LongWritable> {

	@Override
	public void map(LongWritable key, LongWritable value, Context context) 
			throws IOException, InterruptedException{
			//OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		context.write(new HVArraySign(key.get(), value.get(), Config.SMALLSIGN), new LongWritable());
	}
}
