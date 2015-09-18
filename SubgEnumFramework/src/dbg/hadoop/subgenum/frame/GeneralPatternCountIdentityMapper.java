package dbg.hadoop.subgenum.frame;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;


class GeneralPatternCountIdentityMapper extends
		Mapper<NullWritable, LongWritable, NullWritable, LongWritable> {
	
	@Override
	public void map(NullWritable _key, LongWritable _value, Context context)
			throws IOException, InterruptedException {

		context.write(_key, _value);
	}
}