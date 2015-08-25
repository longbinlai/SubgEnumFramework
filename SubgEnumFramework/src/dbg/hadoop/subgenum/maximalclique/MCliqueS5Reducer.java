package dbg.hadoop.subgenum.maximalclique;

import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.utils.HyperVertex;


public class MCliqueS5Reducer
	extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	
	@Override
	public void reduce(LongWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		TLongHashSet set = new TLongHashSet();
		for(LongWritable v : values){
			if(!set.contains(v.get())){
				set.add(v.get());
				context.write(_key, v);
				//System.out.println(HyperVertex.toString(_key.get()) + "\t" + HyperVertex.toString(v.get()));
			}
		}
		set.clear();
		set = null;
	}

}