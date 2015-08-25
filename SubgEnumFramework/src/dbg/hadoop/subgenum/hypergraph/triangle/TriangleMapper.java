package dbg.hadoop.subgenum.hypergraph.triangle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.TwinTwigGenerator;

public class TriangleMapper
		extends Mapper<LongWritable, HyperVertexAdjList, HVArraySign, HVArray> {
	
	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean enableBF;
	
	private TwinTwigGenerator ttwigGen = null;
	// The hypervertex set
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context) 
			throws IOException, InterruptedException{
		if(enableBF){
			ttwigGen = new TwinTwigGenerator(key.get(), value, bloomfilterOpr.get());
		} else{
			ttwigGen = new TwinTwigGenerator(key.get(), value);
		}
		// keyMap = 3 = 011, v2, v3 as the key;
		ttwigGen.genTwinTwigOne(context, Config.LARGESIGN, (byte)3, (byte)0);
	}
	
	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.
		enableBF = conf.getBoolean("enable.bloom.filter", true);
		if(enableBF && bloomfilterOpr == null){
			bloomfilterOpr = new BloomFilterOpr
				(conf.getFloat("bloom.filter.false.positive.rate", (float) 0.001));
			try {
				bloomfilterOpr.obtainBloomFilter(conf);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}		
		}
	}
	
	@Override
	public void cleanup(Context context){
		ttwigGen = null;
	}
}