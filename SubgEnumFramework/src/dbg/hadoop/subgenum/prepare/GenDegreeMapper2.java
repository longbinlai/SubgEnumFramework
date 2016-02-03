package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Get the statics of every node, specifically the degree
 * @author robeen
 *
 */
public class GenDegreeMapper2 extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	@Override
	public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException{

		if(key.compareTo(value) != 0){
			context.write(key, one);
			context.write(value, one);
		}
	}
}