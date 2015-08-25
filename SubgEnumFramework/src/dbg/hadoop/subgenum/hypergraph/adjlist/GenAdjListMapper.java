package dbg.hadoop.subgenum.hypergraph.adjlist;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.utils.HyperVertex;

/**
 * Get the statics of every node, specifically the degree
 * @author robeen
 *
 */
// public class InitMapper extends MapReduceBase implements
public class GenAdjListMapper
		extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	public void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException{
			//OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		if(HyperVertex.Degree(key.get()) > 1){
			context.write(key, value);
		}
		if(value.get() != -1 && key.get() != value.get()){
			context.write(value, key);
		}
	}
}
