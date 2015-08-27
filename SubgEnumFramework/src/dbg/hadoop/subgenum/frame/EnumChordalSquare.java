package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;


public class EnumChordalSquare {
	public static InputInfo inputInfo  = null;
	public static String workdir="";
	public static String filename="";

	public static void main(String[] args) throws Exception {
		inputInfo = new InputInfo(args);
		String workDir = inputInfo.workDir;
		
		if (workDir.toLowerCase().contains("hdfs")) {
			int pos = workDir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(workDir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		// Delete existed output
		if (Utility.getFS().isDirectory(new Path(workDir + "frame.csquare.res"))) {
			Utility.getFS().delete(new Path(workDir + "frame.csquare.res"));
		}
		if (Utility.getFS().isDirectory(new Path(workDir + "frame.csquare.cnt"))) {
			Utility.getFS().delete(new Path(workDir + "frame.csquare.cnt"));
		}
		
		Configuration conf = new Configuration();
		conf.setBoolean("result.compression", inputInfo.isResultCompression);

		long startTime=System.currentTimeMillis();   
			
		String[] Opts = { workDir + "triangle.res", workDir + "frame.csquare.res",	inputInfo.numReducers, inputInfo.jarFile};
		ToolRunner.run(new Configuration(), new EnumChordalSquareDriver(), Opts);
		System.out.println("End of Enumeration");

		long endTime = System.currentTimeMillis();
		System.out.println(" " + (endTime - startTime) / 1000 + "s");
		
		if (inputInfo.isCountPatternOnce && inputInfo.isResultCompression) {
			String[] opts2 = { workDir + "frame.csquare.res", workDir + "frame.csquare.cnt", 
					inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(conf, new GeneralPatternCountDriver(ChordalSquareCountMapper.class), opts2);
		}
	}
}

class EnumChordalSquareDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		// The parameters: <inputfile> <outputDir> <numReducers> <seperator> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		Job job = new Job(conf, "Frame ChordalSquare");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		job.setMapperClass(EnumChordalSquareMapper.class);
		job.setReducerClass(EnumChordalSquareReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		job.setMapOutputKeyClass(HVArray.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(HVArray.class);
		job.setOutputValueClass(HVArray.class);
		
		job.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//client.setConf(conf);
		//JobClient.runJob(job);
		job.waitForCompletion(true);
		return 0;
	}
}

class EnumChordalSquareMapper extends
	Mapper<NullWritable, HVArray, HVArray, LongWritable> {
	
	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(new HVArray(_value.getSecond(), _value.getLast()),
				new LongWritable(_value.getFirst()));
		context.write(new HVArray(_value.getFirst(), _value.getLast()),
				new LongWritable(_value.getSecond()));
		context.write(new HVArray(_value.getFirst(), _value.getSecond()),
				new LongWritable(_value.getLast()));
	}
}

class EnumChordalSquareReducer extends	
	Reducer<HVArray,LongWritable, HVArray, HVArray> {
	
	private static HyperVertexHeap heap = null;
	private static boolean isResultCompression = true;
	
	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		heap.clear();
		for (LongWritable val : values) {
			heap.insert(val.get());
		}  
		heap.sort();
		long[] array = heap.toArrays();
		if(isResultCompression)
			context.write(_key, new HVArray(array));
		else{
			for(int i = 0; i < array.length - 1; ++i){
				for(int j = i + 1; j < array.length; ++j){
					long[] out = { array[i], _key.getFirst(), array[j], _key.getSecond() };
					context.write(new HVArray(),new HVArray(out));
				}
			}
		}
	}
	
	@Override
	public void setup(Context context) {
		heap = new HyperVertexHeap(Config.HEAPINITSIZE);
		isResultCompression = context.getConfiguration().getBoolean(
				"result.compression", true);
	}
	
	@Override
	public void cleanup(Context context){
		heap = null;
	}
} 


class ChordalSquareCountMapper extends
		Mapper<HVArray, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long size = _value.size();
		context.write(NullWritable.get(), new LongWritable(size * (size - 1) / 2));
	}
}