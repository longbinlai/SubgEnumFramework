package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.DoubleIntegerPairWritable;
import dbg.hadoop.subgraphs.utils.HyperVertex;

public class PrepareDataS1Reducer extends Reducer<DoubleIntegerPairWritable, 
	LongWritable, LongWritable, IntWritable> {
	@Override
	public void reduce(DoubleIntegerPairWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException{
		if(_key.getSecond() != -2){
			return;
		}
		long vertex = 0;
		for(LongWritable v : values) {
			if(_key.getSecond() == -2){
				// Encapsulate the vertex id + degree
				vertex = HyperVertex.get(_key.getFirst(), (int)v.get());
				continue;
			}
			context.write(new LongWritable(vertex), new IntWritable((int)v.get()));
		}
	}
}
