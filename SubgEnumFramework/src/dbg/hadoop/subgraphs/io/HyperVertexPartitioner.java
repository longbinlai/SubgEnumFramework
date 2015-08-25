package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import dbg.hadoop.subgraphs.utils.HyperVertex;

public class HyperVertexPartitioner 
extends Partitioner<LongWritable, Writable>{

	@Override
	public int getPartition(LongWritable key, Writable value,
			int numPartitions) {
		// TODO Auto-generated method stub
		return (HyperVertex.VertexID(key.get()) % numPartitions);
	}

}