package dbg.hadoop.subgenum.frame;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Graph;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.CliqueEncoder;
import dbg.hadoop.subgraphs.utils.Utility;

@SuppressWarnings("deprecation")
public class EnumCliqueV2 {

	public static void run(InputInfo inputInfo) throws Exception {
		//inputInfo = new InputInfo(args);
		boolean isCountOnly = inputInfo.isCountOnly;
		String workDir = inputInfo.workDir;
		
		Configuration conf = new Configuration();
		conf.setBoolean("count.only", isCountOnly);
		conf.setStrings("clique.number.vertices", inputInfo.cliqueNumVertices);
		conf.setBoolean("result.compression", inputInfo.isResultCompression);
		conf.setBoolean("count.only", isCountOnly);
		
		FileStatus[] files = Utility.getFS().listStatus(new Path(workDir + Config.cliques));
		for(FileStatus f : files){
			DistributedCache.addCacheFile(f.getPath().toUri(), conf);
		}
		//DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
		//			.toString() + "/" + Config.cliques), conf);
		
		String[] opts = { workDir + "triangle.res", "", workDir + "frame.clique.res",	
					inputInfo.numReducers, inputInfo.jarFile, inputInfo.cliqueNumVertices};
		

		ToolRunner.run(conf, new GeneralDriver("Frame " + inputInfo.cliqueNumVertices + "-Clique", 
			EnumCliqueV2Mapper.class, 
			EnumCliqueV2EnumReducer.class, 
			LongWritable.class, HVArray.class, //OutputKV
			LongWritable.class, HVArray.class, //MapOutputKV
			SequenceFileInputFormat.class, 
			SequenceFileOutputFormat.class,
			null), opts);

	}
	
	public static void countOnce(InputInfo inputInfo) throws Exception {
		if (inputInfo.isCountPatternOnce) {
			String[] opts2 = { inputInfo.workDir + "frame.clique.res", inputInfo.workDir + "frame.clique.cnt", 
					inputInfo.numReducers, inputInfo.jarFile, inputInfo.cliqueNumVertices };
			if (inputInfo.isCountOnly)
				ToolRunner.run(new Configuration(),
								new GeneralPatternCountDriver(
										CliqueCountV2Mapper1.class), opts2);
			else
				ToolRunner.run(new Configuration(),
								new GeneralPatternCountDriver(
										CliqueCountV2Mapper2.class), opts2);
		}
	}
}

class EnumCliqueV2Mapper extends
	Mapper<NullWritable, HVArray, LongWritable, HVArray> {
	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(new LongWritable(_value.getFirst()),
				new HVArray(_value.getSecond(), _value.getLast()));
	}
}

class EnumCliqueV2EnumReducer extends
		Reducer<LongWritable, HVArray, LongWritable, HVArray> {
	private static TLongLongHashMap cliqueMap = null;
	private static TLongHashSet localCliqueSet = null;
	private static boolean isCountOnly = false;
	private static Logger log = Logger.getLogger(EnumCliqueV2EnumReducer.class);
	private static Graph g = null;
	
	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		g.clear();
		localCliqueSet.clear();
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
				//if(HyperVertex.VertexID(_key.get()) == 29974) {
				//	System.out.println("Edge: " + HyperVertex.toString(v1) + "," 
				//			+ HyperVertex.toString(v2));
				//}
				g.addEdge(v1, v2);
			} else {
				//if(HyperVertex.VertexID(_key.get()) == 29974) {
				//	System.out.println("LocalSet: " + HyperVertex.toString(v1) + "," 
				//			+ HyperVertex.toString(v2));
				//}
				//localCliqueSet.add(v1);
				//localCliqueSet.add(v2);
				g.addSetNodes(v1);
				g.addSetNodes(v2);
			}
		}
		//if(localCliqueSet.size() > 20) {
		//	System.out.println(HyperVertex.toString(_key.get()) + " has set size: " + localCliqueSet.size());
		//}
		/*
		if(localCliqueSet.size() <= 50){
			//log.info("Add large local clique set with size : " + localCliqueSet.size());
			long[] array = localCliqueSet.toArray();
			for(int i = 0; i < array.length - 1; ++i){
				for(int j = i + 1; j < array.length; ++j){
					g.addEdge(array[i], array[j]);
				}
			}
			localCliqueSet.clear();
		}*/
		//g.setLocalCliqueSet(localCliqueSet);
		
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("clique.number.vertices"));
		
		long start = System.currentTimeMillis();
		long[] cliqueEnc = g.enumClique(k - 1, _key.get(), isCountOnly);
		long end = System.currentTimeMillis();
		
		if(end - start > 60000) {
			log.info("Time Elapsed: " + (end - start) / 1000 + " s");
			log.info("#Nodes = " + g.getNodesNumber() + "; #Edges = " + g.getUnorientedSize() + 
					"; Set Size = " + localCliqueSet.size());
		}
	
		context.write(_key, new HVArray(cliqueEnc));
	}
	
	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		isCountOnly = conf.getBoolean("count.only", false);
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

class CliqueCountV2Mapper1 extends
		Mapper<LongWritable, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(LongWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), new LongWritable(_value.getFirst()));
		//System.out.println(HyperVertex.VertexID(_key.get()) + "\t" + _value.getFirst());
	}
}

class CliqueCountV2Mapper2 extends
		Mapper<LongWritable, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(LongWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long count = CliqueEncoder.getNumCliquesFromEncodedArrayV2(_value.toArrays());
		context.write(NullWritable.get(), new LongWritable(count));
		//if (_key.get() == HyperVertex.get(100009, 15))
		//System.out.println(HyperVertex.VertexID(_key.get()) + "\t" + count);
	}
}



