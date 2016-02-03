package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dbg.hadoop.subgraphs.io.DoubleIntegerPairWritable;

/**
 * Generate undrirect graph
 * @author robeen
 *
 */
public class UndirectGraphMapper extends Mapper<Text, Text, DoubleIntegerPairWritable, NullWritable> {
	@Override
	public void map(Text _key, Text _value, Context context) throws IOException, InterruptedException{
			//OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		// degree of key is larger than value
		// We deprecate larger degree vertex
		if(_key.toString().startsWith("#") || _key.toString().startsWith("%")
				|| _key.compareTo(_value) == 0){
			return;
		}
		
		int keyInt = Integer.valueOf(_key.toString().trim());
		int valueInt = Integer.valueOf(_value.toString().trim());
		
		if(keyInt < valueInt){
			context.write(new DoubleIntegerPairWritable(keyInt, valueInt), NullWritable.get());
		}
		else{
			context.write(new DoubleIntegerPairWritable(valueInt, keyInt), NullWritable.get());
		}
	}

}
