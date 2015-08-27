package dbg.hadoop.subgenum.hypergraph.adjlist;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

import com.hadoop.compression.lzo.LzoCodec;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;

public class GenAdjListRandomShuffleDriver extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		// The parameters: <inputfile> <outputDir> <numReducers> <jarFile>
		int numReducers = Integer.parseInt(args[2]);

		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");

		Job job = new Job(conf, "HyperGraphGenAdjList Random Shuffle");
		((JobConf)job.getConfiguration()).setJar(args[3]);
		
		job.setMapperClass(GenAdjListRandomShuffleMapper.class);
		job.setReducerClass(GenAdjListRandomShuffleReducer.class);
		
		job.setSortComparatorClass(HVArrayComparator.class);
		
		job.setMapOutputKeyClass(HVArray.class);
		job.setMapOutputValueClass(HyperVertexAdjList.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(HyperVertexAdjList.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType
								(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		job.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}

class GenAdjListRandomShuffleMapper extends
		Mapper<LongWritable, HyperVertexAdjList, HVArray, HyperVertexAdjList> {
	Random rand = null;
	@Override
	public void map(LongWritable key, HyperVertexAdjList value, Context context)
			throws IOException, InterruptedException {

		context.write(new HVArray(key.get(), rand.nextLong()), value);
	}
	@Override
	public void setup(Context context){
		rand = new Random(System.currentTimeMillis());
	}
}

class GenAdjListRandomShuffleReducer extends
		Reducer<HVArray, HyperVertexAdjList, LongWritable, HyperVertexAdjList> {
	@Override
	public void reduce(HVArray key, Iterable<HyperVertexAdjList> values, Context context)
			throws IOException, InterruptedException {
		for(HyperVertexAdjList val : values){
			context.write(new LongWritable(key.getFirst()), val);
		}
	}
}