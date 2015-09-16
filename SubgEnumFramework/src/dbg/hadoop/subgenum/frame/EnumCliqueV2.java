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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Graph;
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
		
		FileStatus[] files = Utility.getFS().listStatus(new Path(workDir + Config.cliques));
		for(FileStatus f : files){
			DistributedCache.addCacheFile(f.getPath().toUri(), conf);
		}

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
	private static Graph g = null;
	
	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		g.clear();
		localCliqueSet.clear();
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
		
		//if(g.getLocalCliqueSetSize() <= 10){
			//log.info("Add large local clique set with size : " + localCliqueSet.size());
			//g.unfoldLocalCliqueSet();
		//}
		
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("clique.number.vertices"));
		long[] cliqueEnc = g.enumClique(k - 1, _key.get(), isCountOnly);
		if(cliqueEnc != null)
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



