package dbg.hadoop.subgenum.qtcounter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class QtS3Mapper
		extends Mapper<LongWritable, LongWritable, NullWritable, LongWritable> {
	
	@Override
	public void map(LongWritable key, LongWritable value, Context context) 
			throws IOException, InterruptedException{
		context.write(NullWritable.get(), value);
	}

}