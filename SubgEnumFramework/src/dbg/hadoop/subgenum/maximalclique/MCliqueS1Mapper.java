package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;


class MCliqueS1Mapper extends
		Mapper<LongWritable, HyperVertexAdjList, HVArraySign, LongWritable> {

	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;
	public static Logger log = Logger.getLogger(MCliqueS1Mapper.class);
	
	// The hypervertex set
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context)
			throws IOException, InterruptedException {
		long largerThanCur[] = value.getLargeDegreeVertices();
		int size = largerThanCur.length;
		long v1 = 0, v2 = 0;
		boolean output = true;
		for (int i = 0; i < size - 1; ++i) {
			for (int j = i + 1; j < size; ++j) {
				v1 = largerThanCur[i];
				v2 = largerThanCur[j];
				if (enableBF
						&& !bloomfilterOpr.get().test(HyperVertex.VertexID(v1),
								HyperVertex.VertexID(v2))) {
					output = false;
				}
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
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.
		log.setLevel(Level.WARN);
		enableBF = conf.getBoolean("enable.bloom.filter", true);
		if (enableBF && bloomfilterOpr == null) {
			bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
					"bloom.filter.false.positive.rate", (float) 0.001));
			try {
				log.info("Get bloomfilter...");
				bloomfilterOpr.obtainBloomFilter(conf);
				assert(bloomfilterOpr.get() != null);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}