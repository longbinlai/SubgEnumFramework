package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GenDegreeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	public void reduce(IntWritable _key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException{
		int degree = 0;

		for(IntWritable v : values) {
			degree += v.get();
		}
		context.write(_key, new IntWritable(degree));
	}
}
