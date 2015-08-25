package dbg.hadoop.subgenum.maximalclique;

import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.Utility;

/**
 * @author robeen
 *
 */
public class MCliqueS5Mapper
		extends Mapper<LongWritable, HyperVertexAdjList, LongWritable, LongWritable> {
	
	private static TLongHashSet outputSet;
	@Override
	public void map(LongWritable key, HyperVertexAdjList adjList, Context context) 
			throws IOException, InterruptedException{
		// If current vertex belong to one clique, 
		// replace it with the clique representative vertex
		outputSet.clear();
		
		long cur = Utility.cliqueMap.contains(key.get()) ? Utility.cliqueMap
				.get(key.get()) : key.get();
		for(long v : adjList.getLargeDegreeVertices()){
			long newV = Utility.cliqueMap.contains(v) ? Utility.cliqueMap.get(v) : v;
			writeOutput(cur, newV, context);
		}
		
		for(long v : adjList.getSmallDegreeVerticesGroup1()){
			long newV = Utility.cliqueMap.contains(v) ? Utility.cliqueMap.get(v) : v;
			writeOutput(cur, newV, context);
		}
	}
	
	private static void writeOutput(long cur, long newV, Context context) 
			throws IOException, InterruptedException{
		if(cur == newV){
			return;
		}
		else{
			boolean reverse = (HyperVertex.compare(cur, newV) < 0) ? false : true;
			long key2Check = reverse ? getKey(newV, cur) : getKey(cur, newV);
			LongWritable output[] = new LongWritable[2];
			if(!outputSet.contains(key2Check)){
				if(reverse){
					output[0] = new LongWritable(newV);
					output[1] = new LongWritable(cur);
				}
				else{
					output[0] = new LongWritable(cur);
					output[1] = new LongWritable(newV);
				}
				outputSet.add(key2Check);
				context.write(output[0], output[1]);
			}
		}
	}
	
	private static long getKey(long v1, long v2){
		long res = (long)HyperVertex.VertexID(v1);
		res = res << 32 | (long)(HyperVertex.VertexID(v2));
		return res;
	}
	
	@Override
	public void setup(Context context){
		outputSet = new TLongHashSet();
		if(Utility.cliqueMap == null){
			try {
				Utility.readCliques(context.getConfiguration(), true);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void cleanup(Context context){
		outputSet.clear();
		outputSet = null;
	}
}