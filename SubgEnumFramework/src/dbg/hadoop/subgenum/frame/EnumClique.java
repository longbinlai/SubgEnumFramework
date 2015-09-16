package dbg.hadoop.subgenum.frame;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Graph;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.CliqueEncoder;

public class EnumClique {

	public static void run(InputInfo inputInfo) throws Exception {
		//inputInfo = new InputInfo(args);
		boolean isCountOnly = inputInfo.isCountOnly;
		String workDir = inputInfo.workDir;
		
		Configuration conf = new Configuration();
		conf.setBoolean("count.only", isCountOnly);
		conf.setStrings("clique.number.vertices", inputInfo.cliqueNumVertices);
		conf.setBoolean("result.compression", inputInfo.isResultCompression);

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

@SuppressWarnings("deprecation")
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

@SuppressWarnings("deprecation")
class EnumCliqueEnumReducer extends
		Reducer<LongWritable, HVArray, LongWritable, HVArray> {
	//private static TLongLongHashMap cliqueMap = null;

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
		long[] cliqueEnc = G.enumCliqueOfSize(k - 1, _key.get());
		if(cliqueEnc != null)
			context.write(_key, new HVArray(cliqueEnc));
	}
}

class CliqueCountMapper1 extends
		Mapper<LongWritable, LongWritable, NullWritable, LongWritable> {
	@Override
	public void map(LongWritable _key, LongWritable _value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), _value);
		//System.out.println(HyperVertex.VertexID(_key.get()) + "\t" + _value.get());
	}
}

class CliqueCountMapper2 extends
		Mapper<LongWritable, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(LongWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long count = CliqueEncoder.getNumCliquesFromEncodedArray(_value.toArrays());
		context.write(NullWritable.get(), new LongWritable(count));
		//System.out.println(HyperVertex.VertexID(_key.get()) + "\t" + count);
	}
}



