package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;
import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexSign;

public class HyperGraphStageOneReducer
	extends Reducer<HVArray, LongWritable, HyperVertexSign, HVArray> {

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
		if(_key.size() == 1){ 
			context.write(new HyperVertexSign(_key.getFirst(), 0), _key);
			//System.out.println("output-key: " + new HyperVertexSign(_key.getFirst(), 0));
			//System.out.println("output-value: " + _key);
		}
		// Means the output adjList only finds one key, it should belong to itself.
		// But we should check it in the second round.
		else if(size == 1){
			context.write(new HyperVertexSign(vertexHeap.getFirst(), 2), 
					new HVArray(vertexHeap.getFirst()));
		}
		// We ouptut the _key.getFirst as the hypervertex id as it is the smallest vertex
		// in the set.
		else{
			context.write(new HyperVertexSign(vertexHeap.getFirst(), 1),
					new HVArray(vertexHeap.toArrays()));
			//System.out.println("output-key: " + new HyperVertexSign(vertexHeap.getFirst(), 1));
			//System.out.println("output-value: " + new HVArray(vertexHeap.toArrays()));
		}
	}
}
