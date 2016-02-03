package dbg.hadoop.subgenum.maximalclique;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.Utility;
import dbg.hadoop.subgraphs.utils.InputInfo;

@SuppressWarnings("deprecation")
public class MaximalCliqueDebug {
	
	public static void main(String[] args) throws Exception{
		run(new InputInfo(args));
	}

	public static void run(InputInfo inputInfo) throws Exception{
		String inputFilePath = inputInfo.inputFilePath;
		
		String jarFile = inputInfo.jarFile;
		String numReducers = inputInfo.numReducers;
		String workDir = inputInfo.workDir;
		
		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		String output = workDir + "clique.debug";
		
		if(Utility.getFS().isDirectory(new Path(output))){
			Utility.getFS().delete(new Path(output));
		}
		
		// The parameters: <graphFileDir> <adjListDir> <outputDir> <numReducers> <jarFile>
		String opts[] = { workDir + Config.cliques, output, numReducers, jarFile};
		
		ToolRunner.run(new Configuration(), new MCliqueDebugDriver(), opts);
	}
}

class MCliqueDebugDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = getConf();
		// The parameters: <adjList> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		
		Job job = new Job(conf, "Maximal Clique Stage Five");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		//JobConf job = new JobConf(getConf(), this.getClass());
		
		job.setMapperClass(MCliqueDebugMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}



class MCliqueDebugMapper
		extends Mapper<LongWritable, HVArray, Text, IntWritable> {
	@Override
	public void map(LongWritable key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		context.write(new Text(HyperVertex.toString(key.get())), 
				new IntWritable(value.size()));
	}
}
