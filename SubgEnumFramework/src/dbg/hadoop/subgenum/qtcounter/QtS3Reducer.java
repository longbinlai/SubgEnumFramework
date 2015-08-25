package dbg.hadoop.subgenum.qtcounter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class QtS3Reducer
	extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {

	@Override
	public void reduce(NullWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long counter = 0;
		for(LongWritable value : values){
			counter += value.get();
		}
		context.write(NullWritable.get(), new LongWritable(counter));
		System.out.println("#cliques : " + counter);
	}
}
