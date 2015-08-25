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
public class GenDegreeMapper extends Mapper<Text, Text, IntWritable, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			//OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		if(key.toString().startsWith("#") || key.toString().startsWith("%")){
			return;
		}
		if(key.compareTo(value) != 0){
			context.write(new IntWritable(Integer.valueOf(key.toString())), one);
			context.write(new IntWritable(Integer.valueOf(value.toString())), one);
		}
	}
}
