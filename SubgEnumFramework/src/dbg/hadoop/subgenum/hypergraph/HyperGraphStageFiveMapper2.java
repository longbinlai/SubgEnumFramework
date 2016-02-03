package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexSign;

/**
 * This mapper deals with the output from stage three.<br>
 * Input: (hid: hyperVertexSet), <br>
 * Output: foreach v in hyperVertexSet, output ((v, -2); hid)
 * @author robeen
 *
 */
public class HyperGraphStageFiveMapper2
		extends Mapper<HyperVertexSign, HVArray, HyperVertexSign, LongWritable> {
	// The hypervertex set
	@Override
	public void map(HyperVertexSign key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		for(long u : value.toArrays()){
			context.write(new HyperVertexSign(u, -2), new LongWritable(key.getVertex()));
		}
	}
}

