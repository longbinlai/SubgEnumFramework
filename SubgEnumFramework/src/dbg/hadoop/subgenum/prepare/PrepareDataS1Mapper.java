package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.DoubleIntegerPairWritable;


/**
 * Get the statics of every node, specifically the degree
 * @author robeen
 *
 **/

public class PrepareDataS1Mapper 
	extends Mapper<Text, Text, DoubleIntegerPairWritable, LongWritable> {

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		if(key.toString().startsWith("#") || key.toString().startsWith("%") ||
				key.compareTo(value) == 0){
			return;
		}
		context.write(new DoubleIntegerPairWritable(Integer.valueOf(key.toString()), -1), 
						new LongWritable(Long.valueOf(value.toString())));
	}
}