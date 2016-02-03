package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.hadoop.compression.lzo.LzoCodec;

@SuppressWarnings("rawtypes")
public class GeneralDriver extends Configured implements Tool{
	
	private String driverName = "General Driver";
	private Class<? extends Mapper> mapperClass1 = null;
	private Class<? extends Mapper> mapperClass2 = null;
	private Class<? extends Reducer> reducerClass = null;
	private Class<? extends Reducer> combinerClass = null;
	private Class<? extends InputFormat> inputFormatClass1 = null;
	private Class<? extends InputFormat> inputFormatClass2 = null;
	private Class<? extends OutputFormat> outputFormatClass = null;
	private Class<? extends Writable> mapOutputKeyClass = null;
	private Class<? extends Writable> mapOutputValueClass = null;
	private Class<? extends Writable> outputKeyClass = null;
	private Class<? extends Writable> outputValueClass = null;
	private Class<? extends RawComparator> sortComparatorClass = null;
	private Class<? extends RawComparator> groupingComparatorClass = null;
	
	/**
	 * @param _name
	 * @param _mapperCls
	 * @param _reducerCls
	 * @param _outputKeyCls
	 * @param _outputValueCls
	 * @param _inputFormatCls
	 * @param _outputFormatCls
	 * @param _sortComparatorCls
	 */
	public GeneralDriver(String _name,
			Class<? extends Mapper> _mapperCls, 
			Class<? extends Reducer> _reducerCls,
			Class<? extends Writable> _outputKeyCls,
			Class<? extends Writable> _outputValueCls,
			Class<? extends InputFormat> _inputFormatCls,
			Class<? extends OutputFormat> _outputFormatCls,
			Class<? extends RawComparator> _sortComparatorCls) {
		this.driverName = _name;
		this.mapperClass1 = _mapperCls;
		this.reducerClass = _reducerCls;
		this.outputKeyClass = _outputKeyCls;
		this.outputValueClass = _outputValueCls;
		this.inputFormatClass1 = _inputFormatCls;
		this.outputFormatClass = _outputFormatCls;
		this.sortComparatorClass = _sortComparatorCls;
	}
	
	
	/**
	 * @param _name
	 * @param _mapperCls
	 * @param _reducerCls
	 * @param _combinerCls
	 * @param _outputKeyCls
	 * @param _outputValueCls
	 * @param _inputFormatCls
	 * @param _outputFormatCls
	 * @param _sortComparatorCls
	 */
	public GeneralDriver(String _name,
			Class<? extends Mapper> _mapperCls, 
			Class<? extends Reducer> _reducerCls,
			Class<? extends Reducer> _combinerCls,
			Class<? extends Writable> _outputKeyCls,
			Class<? extends Writable> _outputValueCls,
			Class<? extends InputFormat> _inputFormatCls,
			Class<? extends OutputFormat> _outputFormatCls,
			Class<? extends RawComparator> _sortComparatorCls) {
		this.driverName = _name;
		this.mapperClass1 = _mapperCls;
		this.reducerClass = _reducerCls;
		this.combinerClass = _combinerCls;
		this.outputKeyClass = _outputKeyCls;
		this.outputValueClass = _outputValueCls;
		this.inputFormatClass1 = _inputFormatCls;
		this.outputFormatClass = _outputFormatCls;
		this.sortComparatorClass = _sortComparatorCls;
	}
	
	/**
	 * @param _name
	 * @param _mapperCls
	 * @param _reducerCls
	 * @param _outputKeyCls
	 * @param _outputValueCls
	 * @param _mapOutputKeyCls
	 * @param _mapOutputValueCls
	 * @param _inputFormatCls
	 * @param _outputFormatCls
	 * @param _sortComparatorCls
	 */
	public GeneralDriver(String _name,
			Class<? extends Mapper> _mapperCls, 
			Class<? extends Reducer> _reducerCls,
			Class<? extends Writable> _outputKeyCls,
			Class<? extends Writable> _outputValueCls,
			Class<? extends Writable> _mapOutputKeyCls,
			Class<? extends Writable> _mapOutputValueCls,
			Class<? extends InputFormat> _inputFormatCls,
			Class<? extends OutputFormat> _outputFormatCls,
			Class<? extends RawComparator> _sortComparatorCls) {
		this.driverName = _name;
		this.mapperClass1 = _mapperCls;
		this.reducerClass = _reducerCls;
		this.outputKeyClass = _outputKeyCls;
		this.outputValueClass = _outputValueCls;
		this.mapOutputKeyClass = _mapOutputKeyCls;
		this.mapOutputValueClass = _mapOutputValueCls;
		this.inputFormatClass1 = _inputFormatCls;
		this.outputFormatClass = _outputFormatCls;
		this.sortComparatorClass = _sortComparatorCls;
	}
	
	/**
	 * @param _name
	 * @param _mapperCls1
	 * @param _mapperCls2
	 * @param _reducerCls
	 * @param _outputKeyCls
	 * @param _outputValueCls
	 * @param _mapOutputKeyCls
	 * @param _mapOutputValueCls
	 * @param _inputFormatCls1
	 * @param _inputFormatCls2
	 * @param _outputFormatCls
	 * @param _sortComparatorCls
	 * @param _groupingComparatorCls
	 */
	public GeneralDriver(String _name,
			Class<? extends Mapper> _mapperCls1, 
			Class<? extends Mapper> _mapperCls2, 
			Class<? extends Reducer> _reducerCls,
			Class<? extends Writable> _outputKeyCls,
			Class<? extends Writable> _outputValueCls,
			Class<? extends Writable> _mapOutputKeyCls,
			Class<? extends Writable> _mapOutputValueCls,
			Class<? extends InputFormat> _inputFormatCls1,
			Class<? extends InputFormat> _inputFormatCls2,
			Class<? extends OutputFormat> _outputFormatCls,
			Class<? extends RawComparator> _sortComparatorCls,
			Class<? extends RawComparator> _groupingComparatorCls) {
		this.driverName = _name;
		this.mapperClass1 = _mapperCls1;
		this.mapperClass2 = _mapperCls2;
		this.reducerClass = _reducerCls;
		this.outputKeyClass = _outputKeyCls;
		this.outputValueClass = _outputValueCls;
		this.mapOutputKeyClass = _mapOutputKeyCls;
		this.mapOutputValueClass = _mapOutputValueCls;
		this.inputFormatClass1 = _inputFormatCls1;
		this.inputFormatClass2 = _inputFormatCls2;
		this.outputFormatClass = _outputFormatCls;
		this.sortComparatorClass = _sortComparatorCls;
		this.groupingComparatorClass = _groupingComparatorCls;
	}
	
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		assert(mapperClass1 != null);
		assert(inputFormatClass1 != null);
		assert(outputFormatClass != null);
		assert(outputKeyClass != null);
		assert(outputValueClass != null);
		
		Configuration conf = getConf();
		// The parameters: <inputDir1> <inputDir2> <outputDir> <numReducers> <jarFile>
		//int numReducers = Integer.parseInt(args[2]);
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");

		Job job = new Job(conf, driverName);
		((JobConf)job.getConfiguration()).setJar(args[4]);
		
		MultipleInputs.addInputPath(job, 
				new Path(args[0]),
				this.inputFormatClass1,
				this.mapperClass1);
		
		if(this.mapperClass2 != null){
			MultipleInputs.addInputPath(job, 
					new Path(args[1]),
					this.inputFormatClass2,
					this.mapperClass2);
		}

		job.setOutputFormatClass(outputFormatClass);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		if(mapOutputKeyClass != null)
			job.setMapOutputKeyClass(mapOutputKeyClass);
		if(mapOutputValueClass != null)
			job.setMapOutputValueClass(mapOutputValueClass);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		
		if(this.sortComparatorClass != null)
			job.setSortComparatorClass(this.sortComparatorClass);
		if(this.groupingComparatorClass != null)
			job.setGroupingComparatorClass(this.groupingComparatorClass);
		
		if(this.combinerClass != null)
			job.setCombinerClass(this.combinerClass);
		if(this.reducerClass != null)
			job.setReducerClass(this.reducerClass);
		
		job.setNumReduceTasks(Integer.parseInt(args[3]));

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
		return 0;
	}
}