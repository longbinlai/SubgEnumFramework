package dbg.hadoop.subgenum.maximalclique;

import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Graph;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;


public class MCliqueS2Reducer
	extends Reducer<LongWritable, HVArray, LongWritable, HVArray> {
	
	private static int cliqueSizeThresh = 20;
	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		Graph graph = new Graph();
		for(HVArray vPair : values){
			graph.addEdge(vPair.getFirst(), vPair.getSecond());
			graph.addEdge(_key.get(), vPair.getFirst());
			graph.addEdge(_key.get(), vPair.getSecond());
		}

		Collection<HyperVertexHeap> cliques = graph.findCliqueCover(cliqueSizeThresh);
		Iterator<HyperVertexHeap> iter = cliques.iterator();
		while(iter.hasNext()){	
			long[] curClique = iter.next().toArrays();
			if(curClique.length < cliqueSizeThresh){
				continue;
			}
			long representVertex = curClique[0];
			context.write(new LongWritable(HyperVertex.get(
						HyperVertex.VertexID(representVertex), true, 
						curClique.length, HyperVertex.Degree(representVertex))), 
						new HVArray(curClique));
		}
		
		graph = null;
		cliques.clear();
		cliques = null;
	}
	
	@Override
	public void setup(Context context){
		cliqueSizeThresh = context.getConfiguration().getInt("mapred.clique.size.threshold", 20);
	}
}
