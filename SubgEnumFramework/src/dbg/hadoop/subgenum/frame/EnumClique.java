package dbg.hadoop.subgenum.frame;

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
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

@SuppressWarnings("deprecation")
public class EnumClique {

	public static void run(InputInfo inputInfo) throws Exception {
		//inputInfo = new InputInfo(args);
		boolean isCountOnly = inputInfo.isCountOnly;
		String workDir = inputInfo.workDir;
		
		Configuration conf = new Configuration();
		conf.setBoolean("count.only", isCountOnly);
		conf.setStrings("clique.number.vertices", inputInfo.cliqueNumVertices);
		if(!isCountOnly){
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
					.toString() + "/" + Config.cliques), conf);
		}
		
		String[] opts = { workDir + "triangle.res", "", workDir + "frame.clique.res",	
					inputInfo.numReducers, inputInfo.jarFile, inputInfo.cliqueNumVertices};
		
		if(isCountOnly){
			ToolRunner.run(conf, new GeneralDriver("Frame " + inputInfo.cliqueNumVertices + "-Clique", 
				EnumCliqueMapper.class, 
				EnumCliqueCountReducer.class, 
				LongWritable.class, LongWritable.class, //OutputKV
				LongWritable.class, HVArray.class, //MapOutputKV
				SequenceFileInputFormat.class, 
				SequenceFileOutputFormat.class,
				null), opts);
		}
		else{
			ToolRunner.run(conf, new GeneralDriver("Frame " + inputInfo.cliqueNumVertices + "-Clique", 
					EnumCliqueMapper.class, 
					EnumCliqueEnumReducer.class, 
					LongWritable.class, HVArray.class, //OutputKV
					LongWritable.class, HVArray.class, //MapOutputKV
					SequenceFileInputFormat.class, 
					SequenceFileOutputFormat.class,
					null), opts);
		}
	}
	
	public static void countOnce(InputInfo inputInfo) throws Exception {
		if (inputInfo.isCountPatternOnce) {
			String[] opts2 = { inputInfo.workDir + "frame.clique.res", inputInfo.workDir + "frame.clique.cnt", 
					inputInfo.numReducers, inputInfo.jarFile, inputInfo.cliqueNumVertices };
			if (inputInfo.isCountOnly)
				ToolRunner.run(new Configuration(),
								new GeneralPatternCountDriver(
										CliqueCountMapper1.class), opts2);
			else
				ToolRunner.run(new Configuration(),
								new GeneralPatternCountDriver(
										CliqueCountMapper2.class), opts2);
		}
	}
}

class EnumCliqueMapper extends
	Mapper<NullWritable, HVArray, LongWritable, HVArray> {
	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(new LongWritable(_value.getFirst()),
				new HVArray(_value.getSecond(), _value.getLast()));
	}
}

class EnumCliqueCountReducer extends 
	Reducer<LongWritable,HVArray, LongWritable, LongWritable> {

	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values, Context context) throws IOException, InterruptedException{
		Graph G = new Graph();
		int cnt=0;
		for (HVArray val : values) {
			cnt = cnt + 1;
			for (int i = 0; i < val.size(); i = i + 2) {
				G.addEdge(val.get(i), val.get(i + 1));
			}
		}
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("clique.number.vertices"));
		long numclique = G.countCliquesOfSize(k - 1);
		context.write(_key,new LongWritable(numclique));
	}
}

class EnumCliqueEnumReducer extends
		Reducer<LongWritable, HVArray, LongWritable, HVArray> {
	private static TLongLongHashMap cliqueMap = null;
	@Override
	public void reduce(LongWritable _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		Graph G = new Graph();
		int cnt = 0;
		for (HVArray val : values) {
			cnt = cnt + 1;
			for (int i = 0; i < val.size(); i = i + 2) {
				G.addEdge(val.get(i), val.get(i + 1));
			}
		}
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("clique.number.vertices"));
		long[] cliqueEnc = G.enumCliqueOfSize(k - 1, _key.get(), cliqueMap);
		context.write(_key, new HVArray(cliqueEnc));
	}
	
	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		for (int i = 0; i < paths.length; ++i) {
			if (paths[i].toString().contains("part-r-")) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[i], conf);
				LongWritable key = null;
				HVArray val = null;
				try {
					key = (LongWritable) reader.getKeyClass().newInstance();
					val = (HVArray) reader.getValueClass().newInstance();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
				while(reader.next(key, val)){
					for(long v : val.toArrays()){
						cliqueMap.put(v, key.get());
					}
				}
				reader.close();
				
			}
		}
	}
}

class CliqueCountMapper1 extends
		Mapper<LongWritable, LongWritable, NullWritable, LongWritable> {
	@Override
	public void map(LongWritable _key, LongWritable _value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), _value);
	}
}

class CliqueCountMapper2 extends
		Mapper<LongWritable, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(LongWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), new LongWritable(
				CliqueEncoder.getNumCliquesFromEncodedArray(_value.toArrays())));
	}
}



