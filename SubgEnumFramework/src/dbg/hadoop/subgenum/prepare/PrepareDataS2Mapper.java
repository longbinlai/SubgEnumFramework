package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.DoubleIntegerPairWritable;


/**
 * Get the statics of every node, specifically the degree
 * @author robeen
 *
 **/

public class PrepareDataS2Mapper 
	extends Mapper<LongWritable, IntWritable, DoubleIntegerPairWritable, LongWritable> {

	@Override
	public void map(LongWritable key, IntWritable value, Context context) 
			throws IOException, InterruptedException{
			//OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		context.write(new DoubleIntegerPairWritable(value.get(), -1), key);
	}
}