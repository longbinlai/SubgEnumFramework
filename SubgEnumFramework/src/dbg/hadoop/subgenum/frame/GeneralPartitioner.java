package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.utils.Utility;

public class GeneralPartitioner {
	
	/**
	 * The run function of general partitioner
	 * @param toPartDir The hdfs dir to partition
	 * @param partThresh Thresh of partition
	 * @param numReducers
	 * @param jarFile
	 * @throws Exception
	 */
	public static void run(String toPartDir, int partThresh,
			String numReducers, String jarFile) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("partition.thresh", partThresh);
		String[] opts = { toPartDir, "", toPartDir + ".part", numReducers, jarFile };
		ToolRunner.run(conf, new GeneralDriver("Frame General Partition", 
				GeneralPartMapper.class, 
				GeneralPartReducer.class, 
				HVArray.class, HVArray.class, //OutputKV
				SequenceFileInputFormat.class, 
				SequenceFileOutputFormat.class,
				HVArrayComparator.class), opts);
	}
}

class GeneralPartMapper extends
		Mapper<HVArray, HVArray, HVArray, HVArray> {

	private static Random rand = null;
	private static int thresh = 2000;

	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		ArrayList<long[]> arrayPartitioner = Utility.partArray(
				_value.toArrays(), thresh);
		for (int i = 0; i < arrayPartitioner.size(); ++i) {
			context.write(
					new HVArray(_key.getFirst(), _key.getSecond(), rand
							.nextLong()), new HVArray(arrayPartitioner.get(i),
							null));
			for (int j = i + 1; j < arrayPartitioner.size(); ++j) {
				context.write(new HVArray(_key.getFirst(), _key.getSecond(),
						rand.nextLong()), new HVArray(arrayPartitioner.get(i),
						arrayPartitioner.get(j)));
			}
		}
	}

	@Override
	public void setup(Context context) {
		thresh = context.getConfiguration().getInt("partition.thresh", 2000);
		rand = new Random(System.currentTimeMillis());
	}
}

class GeneralPartReducer extends
		Reducer<HVArray, HVArray, HVArray, HVArray> {
	@Override
	public void reduce(HVArray key, Iterable<HVArray> values, Context context)
			throws IOException, InterruptedException {
		for (HVArray val : values) {
			context.write(new HVArray(key.getFirst(), key.getSecond()), val);
		}
	}
}