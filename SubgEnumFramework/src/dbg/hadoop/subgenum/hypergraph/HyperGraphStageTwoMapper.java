package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;

/**
 * @author robeen
 *
 */
public class HyperGraphStageTwoMapper
		extends
		Mapper<LongWritable, HyperVertexAdjList, HVArray, LongWritable> {
	
	private static int thresh = 20;
	@Override
	public void map(LongWritable key, HyperVertexAdjList adjList, Context context) 
			throws IOException, InterruptedException{
		long largeThanThis[] = adjList.getLargeDegreeVertices();
		long smallThanThis[] = adjList.getSmallDegreeVerticesGroup1();
		
		if(largeThanThis.length + smallThanThis.length <= thresh){	
			context.write(new HVArray(smallThanThis, largeThanThis), key);
		}
	}
	
	@Override
	public void setup(Context context){
		thresh = context.getConfiguration().getInt("mapred.hypergraph.threshold", 20);
	}
}

