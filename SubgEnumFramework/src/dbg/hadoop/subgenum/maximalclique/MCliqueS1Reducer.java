package dbg.hadoop.subgenum.maximalclique;

import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.utils.Config;



class MCliqueS1Reducer extends
		Reducer<HVArraySign, LongWritable, HVArray, HVArray> {

	@Override
	public void reduce(HVArraySign _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		TLongArrayList list = new TLongArrayList();
		for (LongWritable v : values) {
			if (_key.sign != Config.SMALLSIGN) {
				list.add(v.get());
			}
		}
		context.write(_key.vertexArray, new HVArray(list.toArray()));
	}
}