package dbg.hadoop.subgenum.qtcounter;

import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.BronKerboschCliqueFinder;
import dbg.hadoop.subgraphs.utils.Graph;


public class QtS2Reducer
	extends Reducer<LongWritable, HVArray, LongWritable, LongWritable> {
	
	private static int cliqueSize = 4;
	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		Graph graph = new Graph();
		for(HVArray vPair : values){
			graph.addEdge(vPair.getFirst(), vPair.getSecond());
		}
		long counter = graph.countCliquesOfSize(cliqueSize - 1);
		context.write(_key, new LongWritable(counter));
		graph = null;
	}
	
	@Override
	public void setup(Context context){
		cliqueSize = context.getConfiguration().getInt("mapred.clique.size", 4);
	}
}
