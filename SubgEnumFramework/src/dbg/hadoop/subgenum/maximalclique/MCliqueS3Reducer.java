package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.utils.HyperVertex;


public class MCliqueS3Reducer
	extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	public void reduce(LongWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long largestClique = 0;
		// The vertex will belong to the largest clique
		for(LongWritable value : values){
			if(HyperVertex.Size(largestClique) < HyperVertex.Size(value.get())){
				largestClique = value.get();
			}
		}
		context.write(_key, new LongWritable(largestClique));
	}
}
