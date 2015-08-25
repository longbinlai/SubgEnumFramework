package dbg.hadoop.subgenum.prepare;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.DoubleIntegerPairWritable;

public class UndirectGraphReducer
		extends
		Reducer<DoubleIntegerPairWritable, NullWritable, Text, Text> {
	@Override
	public void reduce(DoubleIntegerPairWritable _key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException{
		context.write(new Text(Integer.toString(_key.getFirst())), new Text(Integer.toString(_key.getSecond())));
	}
}
