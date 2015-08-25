package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;


public class MCliqueS4Reducer
	extends Reducer<LongWritable, LongWritable, LongWritable, HVArray> {
	
	private static int cliqueSizeThresh = 20;
	@Override
	public void reduce(LongWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		int size = 0;
		// The vertex will belong to the largest clique
		HyperVertexHeap heap = new HyperVertexHeap(Config.HEAPINITSIZE);
		for(LongWritable value : values){
			heap.insert(value.get());
			++size;
		}
		if(size >= cliqueSizeThresh){
			heap.sort();
			LongWritable outputKey = new LongWritable(HyperVertex.get
				(HyperVertex.VertexID(heap.getFirst()), true, 
						size, HyperVertex.Degree(heap.getFirst())));
			context.write(outputKey, new HVArray(heap.toArrays()));
			//System.out.println(HyperVertex.toString(outputKey.get()));
			//System.out.println(HyperVertex.toString(outputKey.get()) + "\t" + new HVArray(heap.toArrays()));
		}
		heap.clear();
	}
	
	@Override
	public void setup(Context context){
		cliqueSizeThresh = context.getConfiguration().getInt("mapred.clique.size.threshold", 20);
	}

}
