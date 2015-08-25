package dbg.hadoop.subgenum.prepare;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import dbg.hadoop.subgraphs.io.DoubleIntegerPairComparator;
import dbg.hadoop.subgraphs.io.DoubleIntegerPairWritable;
public class UndirectGraphDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		// The parameters: <inputfile> <outputDir> <numReducers> <seperator> <jarFile>
		int numReducers = Integer.parseInt(args[2]);
		String separator = args[3];
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		//conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
		if(separator.compareTo("comma") == 0){
			conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		}
		if(separator.compareTo("semicolon") == 0){
			conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");
		}
		if(separator.compareTo("space") == 0){
			conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
		}

		Job job = new Job(conf, "UndirectGraph");
		((JobConf)job.getConfiguration()).setJar(args[4]);
		//JobConf job = new JobConf(getConf(), this.getClass());

		job.setMapperClass(UndirectGraphMapper.class);
		job.setReducerClass(UndirectGraphReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setSortComparatorClass(DoubleIntegerPairComparator.class);
		job.setMapOutputKeyClass(DoubleIntegerPairWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//client.setConf(conf);
		//JobClient.runJob(job);
		job.waitForCompletion(true);
		return 0;
	}
}