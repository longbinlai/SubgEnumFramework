package dbg.hadoop.subgenum.hypergraph;

import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexSign;

/**
 * Two different inputs will be received. <br>
 * 1. ((u, -1); adj(u)): The adj list of u; <br>
 * 2. ((u, -2); hyper(u)): The hyper vertex represented by u; <br>
 * We guarantee that second input comes before the first one via secondarysort. <br>
 * The second input only exits when u is the smallest vertex in the vertex set. <br>
 * And if this is the case, we output (u; adj(u) \ hyper(u));
 * @author robeen
 *
 */
public class HyperGraphStageFourReducer
	extends Reducer<HyperVertexSign, HVArray, LongWritable, HVArray> {

	public void reduce(HyperVertexSign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		// We do not receive the second 
		boolean firstRecord = true;
		boolean isClique = (_key.getSign() == -2);
		TLongHashSet set = new TLongHashSet();
		//HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
		//System.out.println("input-key: " + _key);
		if(_key.getSign() == -1){
			return;
		}
		long outputKey = 0;
		for(HVArray value: values){
			if(firstRecord){
				outputKey = _key.getVertex();
				if(isClique){
					for(long u : value.toArrays()){
						if(u != _key.getVertex()){
							//map.put(u, 0);
							set.add(u);
						}
					}
				}
				firstRecord = false;
			}
			else{
				if(isClique){
					int size = 0;
					long[] array = new long[value.size()];
					for(long u : value.toArrays()){
						if(!set.contains(u)){
							array[size++] = u;
						}
					}
					context.write(new LongWritable(outputKey), 
						new HVArray(Arrays.copyOf(array, size)));
				}
				else{
					context.write(new LongWritable(outputKey), value);
				}
			}
		}
		set.clear();
		set = null;	
	}
}
