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

public class EnumHouse {

	public static void run(InputInfo inputInfo) throws Exception {
		String workDir = inputInfo.workDir;
		Configuration conf = new Configuration();

		// Enumerate Square
		if(!inputInfo.isSquareSkip){
			EnumSquare.run(inputInfo);
		}
		
		// Partition the square
		conf.setBoolean("enum.house.square.partition", inputInfo.isSquarePartition);
		if(inputInfo.isSquarePartition){
			GeneralPartitioner.run(workDir + "frame.square.res", inputInfo.squarePartitionThresh, 
					inputInfo.numReducers, inputInfo.jarFile);
		}
		
		// Enumerate House
		String[] opts2 = { workDir + "triangle.res", workDir + "frame.square.res",	
				workDir + "frame.house.res", inputInfo.numReducers, inputInfo.jarFile };
		if(inputInfo.isSquarePartition){
			opts2[1] = workDir + "frame.square.res.part";
		}
		ToolRunner.run(conf, new GeneralDriver("Frame House", 
				EnumHouseTriangleMapper.class,
				EnumHouseSquareMapper.class,
				EnumHouseReducer.class, 
				HVArray.class, HVArray.class, //OutputKV
				HVArraySign.class, HVArray.class, //MapOutputKV
				SequenceFileInputFormat.class, 
				SequenceFileInputFormat.class, 
				SequenceFileOutputFormat.class,
				HVArraySignComparator.class, 
				HVArrayGroupComparator.class), opts2);
		
		//Utility.getFS().delete(new Path(workDir + "frame.square.res.part"));
	}
	
	public static void countOnce(InputInfo inputInfo) throws Exception{
		if (inputInfo.isCountPatternOnce) {
			String[] opts3 = { inputInfo.workDir + "frame.house.res",
					inputInfo.workDir + "frame.house.cnt",
					inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					HouseCountMapper.class), opts3);
		}
	}
}

class EnumHouseTriangleMapper extends
	Mapper<NullWritable, HVArray, HVArraySign, HVArray>{
	
	@Override
	public void map(NullWritable _key, HVArray _value, Context context) 
		throws IOException, InterruptedException {
			context.write(new HVArraySign(_value.getSecond(), _value.getLast(), Config.SMALLSIGN),
					new HVArray(_value.getFirst()));
			context.write(new HVArraySign(_value.getFirst(), _value.getLast(), Config.SMALLSIGN),
					new HVArray(_value.getSecond()));
			context.write(new HVArraySign(_value.getFirst(), _value.getSecond(), Config.SMALLSIGN),
					new HVArray(_value.getLast()));
	}
}

class EnumHouseSquareMapper extends
		Mapper<HVArray, HVArray, HVArraySign, HVArray> {
	private static boolean enableBF = false;
	private static BloomFilterOpr bloomfilterOpr = null;
	private static boolean isSquarePartition = false;
	
	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long[] array = _value.toArrays();
		if(array.length == 0){
			return;
		}
		if(!isSquarePartition)
			handleCompressedSquare(_key.toArrays(), array, context);
		else{
			if(array[0] == -1){
				handleCompressedSquare(_key.toArrays(), Arrays.copyOfRange(array, 1, array.length), context);
			}
			else{
				handleCompressedSquarePart(_key.toArrays(), array, context);
			}
		}
	}
	
	private void handleCompressedSquare(long[] vPair, long[] vertices, Context context) 
			throws IOException, InterruptedException {
		long v2 = vPair[0], v4 = vPair[1];
		int largeThanV2Index = BinarySearch.findLargeIndex(v2, vertices);
		TLongArrayList arrayBuffer = new TLongArrayList();
		boolean isOutput = true;
		for(int i = 0; i < largeThanV2Index; ++i){
			long v1 = vertices[i];
			if (vertices.length - i - 1 > 0) {
				arrayBuffer.add(v4);
				arrayBuffer.addAll(Arrays.copyOfRange(vertices, i + 1,
						vertices.length));
				if (enableBF) {
					isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v1), HyperVertex.VertexID(v2));
				}
				if (isOutput) {
					context.write(new HVArraySign(v1, v2, Config.LARGESIGN),
							new HVArray(arrayBuffer.toArray()));
				}
				arrayBuffer.set(0, v2);
				if (enableBF) {
					isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v1), HyperVertex.VertexID(v4));
				}
				if (isOutput) {
					context.write(new HVArraySign(v1, v4, Config.LARGESIGN),
							new HVArray(arrayBuffer.toArray()));
				}
				arrayBuffer.clear();
			}
		}
		arrayBuffer.clear();
		if(largeThanV2Index > 0){
			for(int i = 0; i < vertices.length; ++i){
				long v3 = vertices[i];
				int sizeOfArray = Math.min(i, largeThanV2Index);
				arrayBuffer.add(v4);
				arrayBuffer.addAll(Arrays.copyOfRange(vertices, 0, sizeOfArray));
				if(v2 < v3){
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v2), HyperVertex.VertexID(v3));
					}
					if(isOutput){
						context.write(new HVArraySign(v2, v3, Config.LARGESIGN),
								new HVArray(arrayBuffer.toArray()));
					}
				}
				else{
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v3), HyperVertex.VertexID(v2));
					}
					if(isOutput){
						context.write(new HVArraySign(v3, v2, Config.LARGESIGN),
								new HVArray(arrayBuffer.toArray()));
					}
				}
				arrayBuffer.set(0, v2);
				if(v3 < v4){
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v3), HyperVertex.VertexID(v4));
					}
					if(isOutput){
						context.write(new HVArraySign(v3, v4, Config.LARGESIGN),
								new HVArray(arrayBuffer.toArray()));
					}
				}
				else{
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v4), HyperVertex.VertexID(v3));
					}
					if(isOutput){
						context.write(new HVArraySign(v4, v3, Config.LARGESIGN),
								new HVArray(arrayBuffer.toArray()));
					}
				}
				arrayBuffer.clear();
			}
		}
	}
	
	private void handleCompressedSquarePart(long[] vPair, long[] groups, Context context) 
			throws IOException, InterruptedException {
		long v2 = vPair[0], v4 = vPair[1];
		long[] group1 = Arrays.copyOfRange(groups, 1, (int)groups[0] + 1);
		long[] group2 = Arrays.copyOfRange(groups, (int)groups[0] + 1, groups.length);
		int largeThanV2Index = BinarySearch.findLargeIndex(v2, group1);
		TLongArrayList arrayBuffer = new TLongArrayList();
		boolean isOutput = true;
		for (int i = 0; i < largeThanV2Index; ++i) {
			long v1 = group1[i];
			arrayBuffer.add(v4);
			arrayBuffer.addAll(group2);
			if (enableBF) {
				isOutput = bloomfilterOpr.get().test(
					HyperVertex.VertexID(v1), HyperVertex.VertexID(v2));
			}
			if(isOutput){
				context.write(new HVArraySign(v1, v2, Config.LARGESIGN),
						new HVArray(arrayBuffer.toArray()));
			}
			arrayBuffer.set(0, v2);
			if (enableBF) {
				isOutput = bloomfilterOpr.get().test(
					HyperVertex.VertexID(v1), HyperVertex.VertexID(v4));
			}
			if(isOutput){
				context.write(new HVArraySign(v1, v4, Config.LARGESIGN),
						new HVArray(arrayBuffer.toArray()));
			}
			arrayBuffer.clear();
		}
		arrayBuffer.clear();
		if (largeThanV2Index > 0) {
			long[] temp = Arrays.copyOfRange(group1, 0, largeThanV2Index);
			for(int i = 0; i < group2.length; ++i){
				long v3 = group2[i];
				arrayBuffer.add(v4);
				arrayBuffer.addAll(temp);
				if(v2 < v3){
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v2), HyperVertex.VertexID(v3));
					}
					if(isOutput){
						context.write(new HVArraySign(v2, v3, Config.LARGESIGN),
								new HVArray(arrayBuffer.toArray()));
					}
				} else {
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v3), HyperVertex.VertexID(v2));
					}
					if(isOutput){
						context.write(new HVArraySign(v3, v2, Config.LARGESIGN),
								new HVArray(arrayBuffer.toArray()));
					}
				}
				arrayBuffer.set(0, v2);
				if(v3 < v4){
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v3), HyperVertex.VertexID(v4));
					}
					if(isOutput){
						context.write(new HVArraySign(v3, v4, Config.LARGESIGN),
								new HVArray(arrayBuffer.toArray()));
					}
				} else {
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(v4), HyperVertex.VertexID(v3));
					}
					if(isOutput){
						context.write(new HVArraySign(v4, v3, Config.LARGESIGN),
								new HVArray(arrayBuffer.toArray()));
					}
				}
				arrayBuffer.clear();
			}
		}
	}
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		enableBF = false;
		isSquarePartition = conf.getBoolean("enum.house.square.partition", false);
		if (enableBF && bloomfilterOpr == null) {
			bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
					"bloom.filter.false.positive.rate", (float) 0.001), Config.BF_TRIANGLE);
			try {
				bloomfilterOpr.obtainBloomFilter(conf);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

class EnumHouseReducer extends
	Reducer<HVArraySign, HVArray, HVArray, HVArray> {
	
	//private static TLongArrayList triangleList = null;
	private static TLongArrayList resList = null;
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> _values, Context context) 
			throws IOException, InterruptedException{
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		long len = 0L;
		
		resList.clear();
		resList.add(len);

		for(HVArray value : _values){
			if(_key.sign == Config.SMALLSIGN){
				resList.add(value.getFirst());
				++len;
			}
			else{
				resList.add(value.size());
				resList.add(value.toArrays());
			}
		}
		resList.set(0, len);
		if(len > 0)
			context.write(new HVArray(_key.vertexArray), new HVArray(resList.toArray()));
	}
	
	@Override
	public void setup(Context context){
		//triangleList = new TLongArrayList();
		resList = new TLongArrayList();
	}
	
	@Override
	public void cleanup(Context context){
		//triangleList.clear();
		//triangleList = null;
		resList.clear();
		resList = null;
	}
}

class HouseCountMapper extends
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
				int squareSize = (int) array[i];
				// long[] squareArray = Arrays.copyOfRange(array, i + 1, i + 1 +
				// squareSize);
				int tmpSize = triSize;
				if (triSet.contains(array[i + 1])) {
					tmpSize -= 1;
				}
				for (int j = i + 2; j < i + 1 + squareSize; ++j) {
					if (triSet.contains(array[j])) {
						count += (tmpSize - 1);
					} else {
						count += tmpSize;
					}
				}
				i += (1 + squareSize);
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