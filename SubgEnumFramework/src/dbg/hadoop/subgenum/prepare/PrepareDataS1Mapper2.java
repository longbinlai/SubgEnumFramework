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

public class PrepareDataS1Mapper2
	extends Mapper<IntWritable, IntWritable, DoubleIntegerPairWritable, LongWritable> {

	@Override
	public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException{
		context.write(new DoubleIntegerPairWritable(key.get(), -1), 
						new LongWritable(Long.valueOf(value.get())));
	}
}