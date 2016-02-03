package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexSign;

/**
 * @author robeen
 *
 */
public class HyperGraphStageFourMapper2
		extends Mapper<HyperVertexSign, HVArray, HyperVertexSign, HVArray> {
	// The hypervertex set
	@Override
	public void map(HyperVertexSign key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		if(key.getSign() == 1){
			context.write(new HyperVertexSign(key.getVertex(), -2), value);
		}
		else{
			context.write(new HyperVertexSign(key.getVertex(), -3), value);
		}
	}
}

