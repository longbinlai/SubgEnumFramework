package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexSign;
import dbg.hadoop.subgraphs.utils.HyperVertex;

/**
 * This mapper deals with the output from stage four.<br>
 * Input: (hid: true_neighbor), true neighbor stands for those vertices which are neighbors of<br>
 * current hypervertex and does not belong to the hyperVertex. <br>
 * Output: foreach v in true_neighbor, output ((v, -1); u)
 * @author robeen
 *
 */
public class HyperGraphStageFiveMapper1
		extends
		Mapper<LongWritable, HVArray, HyperVertexSign, LongWritable> {
	// The hypervertex set
	@Override
	public void map(LongWritable key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		if(value.size() == 0 && HyperVertex.isClique(key.get())){
			long u = HyperVertex.get(HyperVertex.VertexID(key.get()), 
					HyperVertex.Degree(key.get()));
			context.write(new HyperVertexSign(u, -1), key);
		}
		
		for(long u : value.toArrays()){
				context.write(new HyperVertexSign(u, -1), key);
		}
	}
}

