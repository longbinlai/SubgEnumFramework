package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexSign;


public class HyperGraphStageThreeMapper
		extends Mapper<HyperVertexSign, HVArray, HyperVertexSign, HVArray> {
	
	@Override
	public void map(HyperVertexSign key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		context.write(key, value);
	}
}

