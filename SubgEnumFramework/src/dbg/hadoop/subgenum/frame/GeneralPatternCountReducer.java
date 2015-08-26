package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


public class GeneralPatternCountReducer extends
	Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
	private static Logger log = Logger.getLogger(GeneralPatternCountReducer.class);
	@Override
	public void reduce(NullWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long sum = 0L;
		for(LongWritable val : values){
			sum += val.get();
		}
		log.info("# pattern graph: " + sum);
		context.write(NullWritable.get(), new LongWritable(sum));
	}
}