package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class GeneralPatternCountDriver extends Configured implements Tool{
	
	private Class<? extends Mapper> mapperClass = null;
	
	public GeneralPatternCountDriver(Class<? extends Mapper> _mapperClass) {
		mapperClass = _mapperClass;
	}
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		assert(mapperClass != null);
		Configuration conf = getConf();
		// The parameters: <cliqueDir> <outputDir> <numReducers> <jarFile>
		//int numReducers = Integer.parseInt(args[2]);
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
		Job job = new Job(conf, "Frame Pattern Count");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		//SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(this.mapperClass);
		job.setCombinerClass(GeneralPatternCountReducer.class);
		job.setReducerClass(GeneralPatternCountReducer.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}