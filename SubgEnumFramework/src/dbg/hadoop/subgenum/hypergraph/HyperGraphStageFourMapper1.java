package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.io.HyperVertexSign;

/**
 * @author robeen
 *
 */
public class HyperGraphStageFourMapper1
		extends
		Mapper<LongWritable, HyperVertexAdjList, HyperVertexSign, HVArray> {
	@Override
	public void map(LongWritable key, HyperVertexAdjList adjList, Context context) 
			throws IOException, InterruptedException{
		long largeThanThis[] = adjList.getLargeDegreeVertices();
		context.write(new HyperVertexSign(key.get(), -1), new HVArray(largeThanThis));
	}
}

