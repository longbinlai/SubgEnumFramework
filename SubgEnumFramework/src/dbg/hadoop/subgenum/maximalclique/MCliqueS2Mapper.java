package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.HyperVertex;

public class MCliqueS2Mapper
		extends Mapper<HVArray, HVArray, LongWritable, HVArray> {
	private static int cliqueSizeThresh = 20;
	@Override
	public void map(HVArray key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		if(HyperVertex.Degree(key.getFirst()) < cliqueSizeThresh ||
				HyperVertex.Degree(key.getSecond()) < cliqueSizeThresh){
			return;
		}
		for(long u : value.toArrays()){
			if(HyperVertex.Degree(u) >= cliqueSizeThresh){
				context.write(new LongWritable(u), key);
			}
		}
	}
	
	@Override
	public void setup(Context context){
		cliqueSizeThresh = context.getConfiguration().getInt("mapred.clique.size.threshold", 20);
	}
}