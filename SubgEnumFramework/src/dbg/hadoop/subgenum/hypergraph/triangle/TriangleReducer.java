package dbg.hadoop.subgenum.hypergraph.triangle;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Config;


/**
 * Suppose the triangle is (v1, v2, v3) where v1 < v2 < v3 as follows: <br>
 *   v1 <br>
 *  /  \  <br>
 * v2---v3  <br>
 * @author robeen
 *
 */
public class TriangleReducer
	extends Reducer<HVArraySign, HVArray, NullWritable, HVArray> {

	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		long v2 = _key.vertexArray.getFirst();
		long v3 = _key.vertexArray.getSecond();
		if(v2 == v3){
			for(HVArray value: values){
				//System.err.println(new HVArray(value.getFirst(), v2, v3));
				context.write(NullWritable.get(), new HVArray(value.getFirst(), v2, v3));
			}
		}
		else{
			if(_key.sign != Config.SMALLSIGN){
				return;
			}
			for(HVArray value : values){
				if(_key.sign == Config.SMALLSIGN){
					continue;
				}
				else{
					context.write(NullWritable.get(), new HVArray(value.getFirst(), v2, v3));
					//System.err.println(new HVArray(value.getFirst(), v2, v3));
				}
			}
		}
	}
}
