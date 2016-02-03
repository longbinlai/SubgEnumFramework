package dbg.hadoop.subgenum.maximalclique;

import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;


class MCliqueS1Mapper extends
		Mapper<LongWritable, HyperVertexAdjList, HVArraySign, LongWritable> {
	
	private static boolean enableBF;
	private static BloomFilterOpr bloomfilterOpr = null;
	private static int cliqueSizeThresh = 20;
	private static TLongArrayList list = null;
	
	// The hypervertex set
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context)
			throws IOException, InterruptedException {
		
		if(HyperVertex.Degree(key.get()) < cliqueSizeThresh) {
			return;
		}
		
		list.clear();
		long largerThanCur[] = value.getLargeDegreeVertices();
		for(int i = 0; i < largerThanCur.length; ++i) {
			if(HyperVertex.Degree(largerThanCur[i]) >= cliqueSizeThresh){
				list.add(largerThanCur[i]);
			}
		}
		long v1 = 0, v2 = 0;
		int size = largerThanCur.length;
		boolean output = true;
		
		for (int i = 0; i < size - 1; ++i) {
			v1 = largerThanCur[i];
			if(HyperVertex.Degree(v1) < cliqueSizeThresh) {
				continue;
			}
			for (int j = i + 1; j < size; ++j) {
				v2 = largerThanCur[j];
				if(HyperVertex.Degree(v2) < cliqueSizeThresh) {
					continue;
				}
				if(enableBF)
					output = bloomfilterOpr.get().test(HyperVertex.VertexID(v1),
								HyperVertex.VertexID(v2));
				if (output) {
					context.write(new HVArraySign(v1, v2, Config.LARGESIGN),
							key);
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		list = new TLongArrayList();
		cliqueSizeThresh = conf.getInt("mapred.clique.size.threshold", 20);
		enableBF = conf.getBoolean("enable.bloom.filter", true);
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.

		if (enableBF && bloomfilterOpr == null) {
			bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
					"bloom.filter.false.positive.rate", (float) 0.001));
			try {
				bloomfilterOpr.obtainBloomFilter(conf);
				assert(bloomfilterOpr.get() != null);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}