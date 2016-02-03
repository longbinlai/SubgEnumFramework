package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.io.HyperVertexSign;

/**
 * Two different inputs will be received. <br>
 * 1. ((u, -1); hid): The hyperVertex which is adjacent to u; <br>
 * 2. ((u, -2); hid(u)): u's hyperverte id. <br>
 * The second input are guaranteed to come first. <br>
 * In this case, we know that hid and hid(u) are connected. <br>
 * So we output (hid; hid(u)) or (hid(u); hid), the smaller hid comes first
 * @author robeen
 *
 */
public class HyperGraphStageFiveReducer
	extends Reducer<HyperVertexSign, LongWritable, LongWritable, LongWritable> {

	@Override
	public void reduce(HyperVertexSign _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		boolean firstRecord = true;
		LongWritable curV = null;
		for(LongWritable v : values){
			if(firstRecord){
				curV = new LongWritable(v.get());
				firstRecord = false;
			}
			else{
				if(HyperVertex.compare(v.get(), curV.get()) < 0){
					context.write(v, curV);
				}
				else{
					context.write(curV, v);
				}
			}
		}
	}
}
