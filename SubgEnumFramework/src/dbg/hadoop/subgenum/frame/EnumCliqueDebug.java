package dbg.hadoop.subgenum.frame;

import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Graph;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

@SuppressWarnings("deprecation")
public class EnumCliqueDebug {
	
	public static void main(String[] args) throws IOException, Exception {
		run(new InputInfo(args));
	}

	public static void run(InputInfo inputInfo) throws Exception {
		//inputInfo = new InputInfo(args);
		String workDir = inputInfo.workDir;
		
		Configuration conf = new Configuration();
		conf.setStrings("clique.number.vertices", inputInfo.cliqueNumVertices);
		
		FileStatus[] files = Utility.getFS().listStatus(new Path(workDir + Config.cliques));
		for(FileStatus f : files){
			DistributedCache.addCacheFile(f.getPath().toUri(), conf);
		}
		//DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
		//			.toString() + "/" + Config.cliques), conf);
		
		Utility.getFS().delete(new Path(workDir + "frame.clique.v1.debug"));
		Utility.getFS().delete(new Path(workDir + "frame.clique.v2.debug"));
		
		String[] opts = { workDir + "triangle.res", "", workDir + "frame.clique.v1.debug",	
					inputInfo.numReducers, inputInfo.jarFile, inputInfo.cliqueNumVertices};	

		ToolRunner.run(conf, new GeneralDriver("Frame Debug " + inputInfo.cliqueNumVertices + "-Clique V1", 
			EnumCliqueMapper.class, 
			EnumCliqueV1DebugReducer.class, 
			LongWritable.class, Text.class, //OutputKV
			LongWritable.class, HVArray.class, //MapOutputKV
			SequenceFileInputFormat.class, 
			TextOutputFormat.class,
			null), opts);
		
		opts[2] = workDir + "frame.clique.v2.debug";
		
		ToolRunner.run(conf, new GeneralDriver("Frame Debug " + inputInfo.cliqueNumVertices + "-Clique V2", 
				EnumCliqueMapper.class, 
				EnumCliqueV2DebugReducer.class, 
				LongWritable.class, Text.class, //OutputKV
				LongWritable.class, HVArray.class, //MapOutputKV
				SequenceFileInputFormat.class, 
				TextOutputFormat.class,
				null), opts);
	}
	
}

class EnumCliqueV1DebugReducer extends
		Reducer<LongWritable, HVArray, LongWritable, Text> {
	// private static TLongLongHashMap cliqueMap = null;

	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		Graph G = new Graph();
		int cnt = 0;
		for (HVArray val : values) {
			cnt = cnt + 1;
			for (int i = 0; i < val.size(); i = i + 2) {
				long v1 = val.get(i), v2 = val.get(i + 1);
				G.addEdge(v1, v2);
			}
		}
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("clique.number.vertices"));
		long start = System.currentTimeMillis();
		long res = G.countCliquesOfSize(k - 1);
		long end = System.currentTimeMillis();
		if(res > 1000) {
			context.write(new LongWritable(HyperVertex.VertexID(_key.get())), 
				new Text(G.getNodesNumber() + "\t" + G.getUnorientedSize() + "\t" + 0 + "\t" + 
						(end - start) + "\t" + res));
		}
	}
}

class EnumCliqueV2DebugReducer extends
		Reducer<LongWritable, HVArray, LongWritable, Text> {
	private static TLongLongHashMap cliqueMap = null;
	private static TLongHashSet localCliqueSet = null;
	private static Graph g = null;
	
	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		g.clear();
		int cnt = 0;
		boolean mapHasKey = cliqueMap.contains(_key.get());
		boolean noAddEdge = mapHasKey;
		for (HVArray val : values) {
			noAddEdge = mapHasKey;
			long v1 = val.get(0), v2 = val.get(1);
			if (cliqueMap != null && noAddEdge) {
				noAddEdge = cliqueMap.containsKey(v1)
						&& cliqueMap.containsKey(v2)
						&& cliqueMap.get(v1) == cliqueMap.get(v2)
						&& cliqueMap.get(_key.get()) == cliqueMap.get(v1);
			}
			if (!noAddEdge) {
				g.addEdge(v1, v2);
			} else {
				g.addSetNodes(v1);
				g.addSetNodes(v2);
			}
		}
		//g.setLocalCliqueSet(localCliqueSet);
		
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("clique.number.vertices"));
		
		long start = System.currentTimeMillis();
		long[] cliqueEnc = g.enumClique(k - 1, _key.get(), true);
		long end = System.currentTimeMillis();
		
		if(cliqueEnc[0] > 1000) {
			context.write(new LongWritable(HyperVertex.VertexID(_key.get())), 
				new Text(g.getNodesNumber() + "\t" + g.getUnorientedSize() + "\t" + g.getLocalCliqueSetSize() + "\t" + 
						(end - start) + "\t" + cliqueEnc[0]));
		}
	}
	
	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		g = new Graph();
		if (cliqueMap == null) {
			localCliqueSet = new TLongHashSet();
			cliqueMap = new TLongLongHashMap();
			Path[] paths = DistributedCache.getLocalCacheFiles(conf);
			for (int i = 0; i < paths.length; ++i) {
				if (paths[i].toString().contains("part-r-")) {
					LocalFileSystem fs = new LocalFileSystem();
					if (paths[i].toString().contains("part-r-")
							&& !paths[i].toString().endsWith(".crc")) {
						SequenceFile.Reader reader = new SequenceFile.Reader(
								fs, paths[i], conf);
						LongWritable key = null;
						HVArray val = null;
						try {
							key = (LongWritable) reader.getKeyClass()
									.newInstance();
							val = (HVArray) reader.getValueClass()
									.newInstance();
						} catch (InstantiationException e) {
							e.printStackTrace();
						} catch (IllegalAccessException e) {
							e.printStackTrace();
						}
						while (reader.next(key, val)) {
							for (long v : val.toArrays()) {
								cliqueMap.put(v, key.get());
							}
						}
						reader.close();
					}
				}
			}
		}
	}
	
	@Override
	public void cleanup(Context context){
		if(cliqueMap != null){
			cliqueMap.clear();
			cliqueMap = null;
		}
	}
}




