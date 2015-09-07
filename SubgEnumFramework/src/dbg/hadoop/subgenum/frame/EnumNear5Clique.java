package dbg.hadoop.subgenum.frame;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayGroupComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.utils.BinarySearch;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class EnumNear5Clique {

	public static void run(InputInfo inputInfo) throws Exception {
		String workDir = inputInfo.workDir;
		Configuration conf = new Configuration();
		if(!inputInfo.isFourCliqueSkip){
			inputInfo.cliqueNumVertices = "4"; // Make sure that 4clique is enumerated
			EnumClique.run(inputInfo);
		}

		// Enumerate House
		String[] opts2 = { workDir + "triangle.res", workDir + "frame.clique.res",	
				workDir + "frame.near5clique.res", inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new GeneralDriver("Frame Near5Clique", 
				EnumHouseTriangleMapper.class,
				EnumNear5CliqueMapper.class,
				EnumNear5CliqueReducer.class, 
				HVArray.class, HVArray.class, //OutputKV
				HVArraySign.class, HVArray.class, //MapOutputKV
				SequenceFileInputFormat.class, 
				SequenceFileInputFormat.class, 
				SequenceFileOutputFormat.class,
				HVArraySignComparator.class, 
				HVArrayGroupComparator.class), opts2);			
	}

	public static void countOnce(InputInfo inputInfo) throws Exception{
		if (inputInfo.isCountPatternOnce) {
			String[] opts3 = { inputInfo.workDir + "frame.near5clique.res",
					inputInfo.workDir + "frame.near5clique.cnt",
					inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					Near5CliqueCountMapper.class), opts3);
		}
	}

}

class EnumNear5CliqueMapper extends 
	Mapper<LongWritable, HVArray, HVArraySign, HVArray> {

	private static TLongLinkedList list = null;
	private static isCompress = false;

	@Override
	public void map(LongWritable _key, HVArray _value, Context context)
		throws IOException, InterruptedException {
		if(!isCompress){
			long[] array = Arrays.copyOfRange(_value.toArrays(), 3, _value.size());
			handleCliqueNorm(_key, array, context);
		}
		else {
			int cliqueSize = (int) _value.get(2);
			long[] cliqueArray = Arrays.copyOfRange(_value.toArrays(), 3, cliqueSize);
			long[] normArray = Arrays.copyOfRange(_value.toArrays(), 3 + cliqueSize, _value.size());
			handleCliqueCompress(_key, cliqueArray, context);
			handleCliqueNorm(_key, normArray, context);
		}
	}

	private void handleCliqueNorm(LongWritable key, long[] array, Context context)
		throws IOException, InterruptedException {
		for(int i = 0; i < array.length; i += 3){
			context.write(new HVArraySign(key.get(), array[i], Config.LARGESIGN), 
				new HVArray(array[i + 1], array[i + 2]));
			context.write(new HVArraySign(key.get(), array[i + 1], Config.LARGESIGN), 
				new HVArray(array[i], array[i + 2]));
			context.write(new HVArraySign(key.get(), array[i + 2], Config.LARGESIGN), 
				new HVArray(array[i], array[i + 1]));
			context.write(new HVArraySign(array[i], array[i + 1], Config.LARGESIGN), 
				new HVArray(key.get(), array[i + 2]));
			context.write(new HVArraySign(array[i], array[i + 2], Config.LARGESIGN), 
				new HVArray(key.get(), array[i + 1]));
			context.write(new HVArraySign(array[i + 1], array[i + 2], Config.LARGESIGN), 
				new HVArray(key.get(), array[i]));
		}
	}

	private void handleCliqueCompress(LongWritable key, long[] array, Context context)
		throws IOException, InterruptedException {
		list.clear();
		list.addAll(array);
		for(int i = 0; i < array.lenth - 1; ++i){
			for(int j = 0 i < array.length; ++j){
				// Remove i, j
				list.removeAt(i);
				list.removeAt(j);
				context.write(new HVArraySign(array[i], array[j], Config.LARGESIGN), 
					new HVArray(list.toArray()));
				// Add back i, j
				list.insert(i, array[i]);
				list.insert(j, array[j]);
			}
		}	
	}

	@Override
	public void setup(Context context){
		isCompress = context.getBoolean("result.compression", false);
		list = new TLongLinkedist();
	}

	@Override
	public void cleanup(Context context){
		list.clear();
		list = null;
	}
}

class EnumNear5CliqueReducer extends
	Reducer<HVArraySign, HVArray, HVArray, HVArray> {
	
	//private static TLongArrayList triangleList = null;
	private static TLongArrayList resList = null;
	private static isCompress = false;
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> _values, Context context) 
			throws IOException, InterruptedException{
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		long len = 0L;
		
		resList.clear();
		if(isCompress)
			resList.add(len);

		for(HVArray value : _values){
			if(_key.sign == Config.SMALLSIGN){
				resList.add(value.getFirst());
				++len;
			}
			else {
				if(isCompress) {
					//resList.add(value.size());
					if(value.size() != 2)
						resList.add(value.size());
					resList.add(value.toArrays());
				}
				else {
					TLongIterator iter = resList.iterator();
					while(iter.hasNext()) {
						long v1 = iter.next();
						long v3 = value.getFirst();
						long v4 = value.getSecond();
						if(v1 != v3 && v1 != v4) {
							context.write(_key, new HVArray(v1, v3, v4));
						}
					}
				}	
			}
		}
		if(isCompress) {
			resList.set(0, len);
			if(len > 0)
				context.write(new HVArray(_key.vertexArray), new HVArray(resList.toArray()));
		}
	}
	
	@Override
	public void setup(Context context){
		//triangleList = new TLongArrayList();
		resList = new TLongArrayList();
		isCompress = context.getConfiguration().getBoolean("result.compression", false);
	}
	
	@Override
	public void cleanup(Context context){
		//triangleList.clear();
		//triangleList = null;
		resList.clear();
		resList = null;
	}
}

class Near5CliqueCountMapper extends
		Mapper<HVArray, HVArray, NullWritable, LongWritable> {
	
	private static TLongHashSet triSet = null;
	
	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long count = 0L;
		long[] array = _value.toArrays();
		int triSize = (int)array[0];
		if(triSize == 0){
			return;
		}
		triSet.clear();
		for(int i = 1; i < triSize + 1; ++i){
			triSet.add(array[i]);
		}

		try {
			int i = triSize + 1;
			while (i < array.length) {
				// This is not a size
				if(array[i] >= (long) 1 << 26) {
					count += triSize;
					if(triSet.contains(array[i])) {
						count -= 1;
					}
					if(triSet.contains(array[i + 1])) {
						count -= 1;
					}
					i += 2;
				} else {
					int cliqueSize = (int) array[i];
					long[] cliqueArray = Arrays.copyOfRange(array, i + 1, cliqueSize);
				    int num = 0;
				    for(int i = 0; i < cliqueArray.length; ++i) {
				    	if(triSet.contains(cliqueArray[i])) {
				    		++num;
				    	}
				    }
				    count += CliqueEncoder.binom(cliqueSize, 2);
				    count -= CliqueEncoder.binom(num, 2) * 2;
				    count -= num * (cliqueSize - num);
					i += cliqueSize + 1;
				}
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println(HyperVertex.HVArrayToString(array));
			System.exit(1);
		}
		context.write(NullWritable.get(), new LongWritable(count));
	}
	
	@Override
	public void setup(Context context){
		triSet = new TLongHashSet();
	}
	
	@Override
	public void cleanup(Context context){
		triSet.clear();
		triSet = null;
	}
}