package dbg.hadoop.subgenum.prepare;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

public class SampleGraph{
	private static InputInfo inputInfo = null;
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
		inputInfo = new InputInfo(args);
		String inputFilePath = inputInfo.inputFilePath;
		String jarFile = inputInfo.jarFile;
		String dir = inputInfo.workDir;
		float sampleRate = inputInfo.sampleRate;
		String sampleSuffix = String.valueOf((int)(100 * sampleRate));
		String sampleType = inputInfo.sampleType;
		
		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		String realInputFileDir = dir + Config.preparedFileDir;
		String sampleVertexPath = "sample/" + "vertex_" + sampleSuffix;
		String outputDir = "";
		if (!dir.isEmpty()) {
			outputDir = dir.substring(0, dir.length() - 1);
		}
		
		outputDir += (sampleType + sampleSuffix) + "/" + Config.undirectGraphDir;
		
		if(Utility.getFS().isDirectory(new Path(outputDir)))
			Utility.getFS().delete(new Path(outputDir));
		
		Configuration conf = new Configuration();
		
		if(sampleType.compareTo("v") == 0){
			Utility.sampleVertex(sampleRate, sampleVertexPath, dir);
			DistributedCache.addCacheFile(new URI(new Path(dir + sampleVertexPath)
													.toUri().toString()), conf);
			
			String[] opts = { realInputFileDir, outputDir, jarFile };
			ToolRunner.run(conf, new SampleGraphByVertexDriver(), opts);
		}
		else{
			String[] opts = { realInputFileDir, outputDir, jarFile, sampleSuffix };
			ToolRunner.run(conf, new SampleGraphByEdgeDriver(), opts);
		}
	}
}

class SampleGraphByVertexDriver extends Configured implements Tool{
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <inputfile> <outputDir> <jarFile>

		Job job = new Job(conf, "SampleGraphByVertex");
		((JobConf)job.getConfiguration()).setJar(args[2]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		job.setMapperClass(SampleGraphByVertexMapper.class);
		//job.setCombinerClass(InitReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}

class SampleGraphByVertexMapper extends Mapper<LongWritable, LongWritable, IntWritable, IntWritable> {
	
	private static TIntArrayList sampleVertices = null;
	@Override
	public void map(LongWritable _key, LongWritable _value, Context context) throws IOException, InterruptedException{
		if(sampleVertices.binarySearch(HyperVertex.VertexID(_key.get())) >= 0 &&
				sampleVertices.binarySearch(HyperVertex.VertexID(_value.get())) >= 0){
			context.write(new IntWritable(HyperVertex.VertexID(_key.get())), 
					new IntWritable(HyperVertex.VertexID(_value.get())));
		}
		
	}
	
	@Override
	protected void setup(Context context) throws IOException {
		sampleVertices = new TIntArrayList();

		Path[] paths = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		for (int i = 0; i < paths.length; ++i) {
			File f = new File(paths[i].toString());
			if (f.toString().contains("vertex_")) {
				BufferedReader reader = new BufferedReader(new FileReader(f));
				String line = "";
				while ((line = reader.readLine()) != null) {
					sampleVertices.add(Integer.valueOf(line));
				}
				sampleVertices.sort();
				reader.close();
			}
		}
	}
	
	@Override
	protected void cleanup(Context context){
		sampleVertices.clear();
		sampleVertices = null;
	}
}

class SampleGraphByEdgeDriver extends Configured implements Tool{
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <inputfile> <outputDir> <jarFile> <graph.sample.rate>
		conf.setInt("graph.sample.value", Integer.valueOf(args[3]));
		
		Job job = new Job(conf, "SampleGraphByEdge");
		((JobConf)job.getConfiguration()).setJar(args[2]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		job.setMapperClass(SampleGraphByEdgeMapper.class);
		//job.setCombinerClass(InitReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}

class SampleGraphByEdgeMapper extends Mapper<LongWritable, LongWritable, IntWritable, IntWritable> {
	private Random rand = null;
	private static int sampleValue = 20;;
	@Override
	public void map(LongWritable _key, LongWritable _value, Context context) 
			throws IOException, InterruptedException{
		if(rand.nextInt(100) < sampleValue){
			context.write(new IntWritable(HyperVertex.VertexID(_key.get())), 
					new IntWritable(HyperVertex.VertexID(_value.get())));
		}
		
	}
	
	@Override
	protected void setup(Context context) throws IOException{
		rand = new Random(System.currentTimeMillis());
		sampleValue = context.getConfiguration().getInt("graph.sample.value", 20);
	}
}
