package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArray;

/**
 * Merge duplicated results.
 * @author robeen
 *
 */
public class HyperGraphStageSixCombiner
	extends Reducer<HVArray, NullWritable, HVArray, NullWritable> {

	@Override
	public void reduce(HVArray _key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		context.write(_key, NullWritable.get());
	}
}