package dbg.hadoop.subgenum.hypergraph;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexSign;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;

public class HyperGraphStageTwoReducer extends Reducer
	<HVArray, LongWritable, HyperVertexSign, HVArray> {

	public void reduce(HVArray _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int size = 0;
		HyperVertexHeap vertexHeap = new HyperVertexHeap(Config.HEAPINITSIZE);
		for(LongWritable value: values){
			++size;
			vertexHeap.insert(value.get());
		}
		vertexHeap.sort();
		
		// Means we donot output the adjList, the vertex should belong to itself.
		// We donot need to check it anymore
		if(size == 1){ 
			context.write(new HyperVertexSign(vertexHeap.getFirst(), 2), 
					new HVArray(vertexHeap.getFirst()));
		}
		// We ouptut the _key.getFirst as the hypervertex id as it is the smallest vertex
		// in the set.
		else{
			context.write(new HyperVertexSign(vertexHeap.getFirst(), 0), 
					new HVArray(vertexHeap.toArrays()));
		}
	}

}
