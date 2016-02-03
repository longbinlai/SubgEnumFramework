package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArray;

/**
 * Merge duplicated results.
 * @author robeen
 *
 */
public class HyperGraphStageSixReducer
	extends Reducer<HVArray, NullWritable, LongWritable, LongWritable> {

	@Override
	public void reduce(HVArray _key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		context.write(new LongWritable(_key.getFirst()), new LongWritable(_key.getSecond()));
		//System.out.println(HyperVertex.toString(_key.getFirst()) + ";" + 
		//		HyperVertex.toString(_key.getSecond()));
	}
}