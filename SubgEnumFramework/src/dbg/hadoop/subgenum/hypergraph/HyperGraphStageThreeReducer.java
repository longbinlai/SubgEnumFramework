package dbg.hadoop.subgenum.hypergraph;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HyperVertexSign;
import dbg.hadoop.subgraphs.utils.HyperVertex;


/***
 * This reducer deals with the inputs from both stage one and two. <br>
 * There are two cases: <br>
 * 
 * CASE 1: The vertex has been assigned to a clique, or its neighbor <br>
 * size is too large, not assign then. We directly output the key/value in this case. <br>
 * <br>
 * CASE 2: The vertex has been assigned to a vertexSet, or not assign <br>
 * If assigned to a vertexSet, two records will be received, they are: <br>
 * ((u, 0); set) and ((u, 2); {u}). By using secondary sort, it is guaranteed <br>
 * that the first record comes before the second one. We output ((u, 0); set) then. <br>
 * If assigned to itself, only ((u, 2); {u}) will be received, and we output ((u; 0); {u}).
 *  
 * @author robeen
 *
 */
public class HyperGraphStageThreeReducer
	extends Reducer<HyperVertexSign, HVArray, HyperVertexSign, HVArray> {

	public void reduce(HyperVertexSign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {	
		// CASE 1
		if(_key.getSign() == 1 || _key.getSign() == 0){
			for(HVArray value: values){
				long hyperVertex = HyperVertex.get(_key.getVertex(), value.size(), (_key.getSign() == 1));
				context.write(new HyperVertexSign(hyperVertex, _key.getSign()), value);
				return;
			}
		}
		// CASE 2
		else{
			int size = 0;
			for(HVArray value: values){
				if(value.size() > 1 && value.getFirst() != _key.getVertex()){
					long hyperVertex = HyperVertex.get(_key.getVertex(), value.size(), (_key.getSign() == 1));
					context.write(new HyperVertexSign(hyperVertex, _key.getSign()), value);
					return;
				}
				else{
					++size;
				}
			}
			if(size == 2){
				long hyperVertex = HyperVertex.get(_key.getVertex(), 1, false);
				context.write(new HyperVertexSign(hyperVertex, 0), 
						new HVArray(_key.getVertex()));
			}
		}
	}
}
