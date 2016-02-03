package dbg.hadoop.subgenum.frame;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.linked.TLongLinkedList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayGroupComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

public class EnumNear5Clique {

	public static void run(InputInfo inputInfo) throws Exception {
		String workDir = inputInfo.workDir;
		Configuration conf = new Configuration();
		conf.setBoolean("result.compression", inputInfo.isResultCompression);
		conf.setBoolean("count.only", inputInfo.isCountOnly);
		boolean isCountOnly = inputInfo.isCountOnly;
		
		if(!inputInfo.isFourCliqueSkip){
			inputInfo.cliqueNumVertices = "4"; // Make sure that 4clique is enumerated
			inputInfo.isCountOnly = false; // We cannot apply count-only to intermediate results
			if(inputInfo.isResultCompression) {
				EnumCliqueV2.run(inputInfo);
			}
			else {
				EnumClique.run(inputInfo);
			}
		}
		inputInfo.isCountOnly = isCountOnly;
		
		FileStatus[] files = Utility.getFS().listStatus(new Path(workDir + Config.cliques));
		for(FileStatus f : files){
			DistributedCache.addCacheFile(f.getPath().toUri(), conf);
		}

		String[] opts = { workDir + "triangle.res", workDir + "frame.clique.res",	
				workDir + "frame.near5clique.res", inputInfo.numReducers, inputInfo.jarFile };
		if (!inputInfo.isCountOnly) {
			ToolRunner.run(conf, new GeneralDriver(
					"Frame Near5Clique",
					EnumHouseTriangleMapper.class,
					EnumNear5CliqueMapper.class,
					EnumNear5CliqueReducer.class,
					HVArray.class,
					HVArray.class, // OutputKV
					HVArraySign.class,
					HVArray.class, // MapOutputKV
					SequenceFileInputFormat.class,
					SequenceFileInputFormat.class,
					SequenceFileOutputFormat.class,
					HVArraySignComparator.class, HVArrayGroupComparator.class),
					opts);
		} else {
			ToolRunner.run(conf, new GeneralDriver(
					"Frame Near5Clique",
					EnumHouseTriangleMapper.class,
					EnumNear5CliqueMapper.class,
					EnumNear5CliqueCountReducer.class,
					NullWritable.class,
					LongWritable.class, // OutputKV
					HVArraySign.class,
					HVArray.class, // MapOutputKV
					SequenceFileInputFormat.class,
					SequenceFileInputFormat.class,
					SequenceFileOutputFormat.class,
					HVArraySignComparator.class, HVArrayGroupComparator.class),
					opts);
		}
	}

	public static void countOnce(InputInfo inputInfo) throws Exception{
		if (inputInfo.isCountPatternOnce) {
			String[] opts = { inputInfo.workDir + "frame.near5clique.res",
					inputInfo.workDir + "frame.near5clique.cnt",
					inputInfo.numReducers, inputInfo.jarFile };
			if(inputInfo.isCountOnly) {
				ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
						GeneralPatternCountIdentityMapper.class), opts);
			}
			else if(inputInfo.isResultCompression) {
				ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					Near5CliqueCountMapper.class), opts);
			}
			else {
				System.out.println("Not count needed");
			}
		}
	}

}

/**
 * There are three forms of clique storage. 
 * Suppose the key is v1 <br> <br>
 * CASE I CliqueNorm: Cliques are stored without compression, for example:
 * (v2, v3, v4, v5, v6, v7). In this case, every three vertices in a 
 * row form a 4-clique with v1, such as (v1, v2, v3, v4) and (v1, v5, v6, v7) <br> <br>
 * CASE II CliquePartialCompress: Cliques are stored with partial compression, for example:
 * ([v2], v3, v4, v5, v6, v7), where [v2] is the prefix array and v3, v4, v5, v6, v7 ... form
 * a large clique. In this case, v1 and v2, together with every two vertices in v3, v4, ..., v7, ...
 * form a 4-clique, such as (v1, v2, v3, v4), (v1, v2, v3, v5), ... (v1, v2, v6, v7), ... <br> <br>
 * CASE III CliqueCompress:  Cliques are stored with full compression, for example: 
 * (v2, v3, v4, v5, v6, v7), where all vertices form a large clique. In this case, every three vertices
 * in the array together with v1 form a 4-clique.
 * @author robeen
 *
 */
class EnumNear5CliqueMapper extends 
	Mapper<LongWritable, HVArray, HVArraySign, HVArray> {
	
	private static TLongLongHashMap cliqueMap = null;
	private static TLongLinkedList list = null;
	private static boolean isCompress = false;

	@Override
	public void map(LongWritable _key, HVArray _value, Context context)
		throws IOException, InterruptedException {
		long[] array = _value.toArrays();
		//System.out.println("full array: " + HyperVertex.HVArrayToString(array));
		if(!isCompress){
			long[] trueArray = Arrays.copyOfRange(array, 3, _value.size());
			handleCliqueNorm(_key, trueArray, context);
		}
		else {
			int cliqueSize = (int) _value.get(2);
			//long[] cliqueArray = Arrays.copyOfRange(_value.toArrays(), 3, 3 + cliqueSize);
			if(cliqueSize == -1) {
				long[] normArray = Arrays.copyOfRange(array, 3, _value.size());
				handleCliqueNorm(_key, normArray, context);
			}
			else {
				if(cliqueSize > 0) {
					long[] cliqueArray = Arrays.copyOfRange(array, 3, 3 + cliqueSize);
					//System.out.println("cliqueArray: " + HyperVertex.HVArrayToString(cliqueArray));
					handleCliqueCompress(_key, cliqueArray, context);
				}
				int index = 3 + cliqueSize;
				while(index < array.length){
					int len = (int) array[index];
					int nonCliqueVerticesSize = (int) array[index + 1];
					long[] prefix = Arrays.copyOfRange(array, index + 2, index
							+ 2 + nonCliqueVerticesSize);
					long[] cliqueArray = Arrays.copyOfRange(array, index + 2
							+ nonCliqueVerticesSize, index + len + 1);
					//System.out.println("prefix: " + HyperVertex.HVArrayToString(prefix));
					//System.out.println("prefix: " + HyperVertex.HVArrayToString(cliqueArray));
					handleCliquePartialCompress(_key, prefix, cliqueArray,
							context);
					index += len + 1;
				}
			}
		}
	}

	private void handleCliqueNorm(LongWritable key, long[] array, Context context)
		throws IOException, InterruptedException {
		if(array.length < 3) {
			return;
		}
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
		long v0 = key.get();
		long v1 = 0L;
		//System.out.println("key:" + HyperVertex.toString(key.get()));
		//System.out.println("cliqueMap:" + HyperVertex.toString(cliqueMap.get(key.get())));
		if (HyperVertex.VertexID(cliqueMap.get(key.get())) 
				== HyperVertex.VertexID(key.get())) {
			for (int i = 0; i < array.length; ++i) {
				v1 = array[i];
				list.removeAt(i);
				context.write(new HVArraySign(v0, v1, Config.LARGESIGN),
						new HVArray(list.toArray()));
				list.insert(i, v1);
			}
			list.insert(0, v0);
			//list.insert(0, -1);
			for (int i = 0; i < array.length - 1; ++i) {
				for (int j = i + 1; j < array.length; ++j) {
					// Remove i, j
					v0 = array[i];
					v1 = array[j];
					list.removeAt(i + 1);
					list.removeAt(j);
					context.write(new HVArraySign(v0, v1, Config.LARGESIGN),
							new HVArray(list.toArray()));
					// Add back i, j
					list.insert(i + 1, v0);
					list.insert(j + 1, v1);
				}
			}
		}
	}
	
	private void handleCliquePartialCompress(LongWritable key, long[] prefix, long[] cliqueArray, 
			Context context) throws IOException, InterruptedException {
		int k = 3;
		int n = cliqueArray.length;
		int l = k - prefix.length;
		if(l == 0) { // Which mean the prefix with the key form a 4 -clique already
			handleCliqueNorm(key, prefix, context);
			return;
		}
		long[] curClique = new long[k];
		for(int i = 0; i < prefix.length; ++i) {
			curClique[i] = prefix[i];
		}
		// Enumerating l vertices out of n in cliqueArray
		BigInteger two = BigInteger.valueOf(2);
		BigInteger start = two.pow(l).subtract(BigInteger.ONE);
		BigInteger end = BigInteger.ONE;
		for(int i = 1; i <= l; ++i) {
			end = end.add(two.pow(n - i));
		}
		
		BigInteger x = start;
		BigInteger u = BigInteger.ZERO;
		BigInteger v = BigInteger.ZERO;
		BigInteger y = BigInteger.ZERO;
		BigInteger tmp = x;
		int count = prefix.length;
		
		long[] tmpClique = new long[3];
		while(y.compareTo(end) < 0) {
			count = prefix.length;
			while(tmp.compareTo(BigInteger.ZERO) != 0) {
				int index = tmp.getLowestSetBit();
				tmp = tmp.flipBit(index);
				assert(count < k);
				curClique[count++] = cliqueArray[index];
				if(count == k) {
					System.arraycopy(curClique, 0, tmpClique, 0, k);
					Arrays.sort(tmpClique);
					handleCliqueNorm(key, tmpClique, context);
				}
			}
			
			u = x.and(x.negate()); // u = x & (-x)
			v = x.add(u);  // v = x + u
			y = v.add(v.xor(x).divide(u).shiftRight(2)); // y = v + (((v^x) / u) >> 2)
			x = y;
			tmp = x;
		}
	}

	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		isCompress = conf.getBoolean("result.compression", false);
		list = new TLongLinkedList();
		if (cliqueMap == null) {
			cliqueMap = new TLongLongHashMap();
			Path[] paths = DistributedCache.getLocalCacheFiles(conf);
			for (int i = 0; i < paths.length; ++i) {
				if (paths[i].toString().contains("part-r-")) {
					LocalFileSystem fs = new LocalFileSystem();
					if (paths[i].toString().contains("part-r-")
							&& !paths[i].toString().endsWith(".crc")) {
						SequenceFile.Reader reader = new SequenceFile.Reader(
								fs, paths[i], conf);
						LongWritable key = null;
						HVArray val = null;
						try {
							key = (LongWritable) reader.getKeyClass()
									.newInstance();
							val = (HVArray) reader.getValueClass()
									.newInstance();
						} catch (InstantiationException e) {
							e.printStackTrace();
						} catch (IllegalAccessException e) {
							e.printStackTrace();
						}
						while (reader.next(key, val)) {
							for (long v : val.toArrays()) {
								cliqueMap.put(v, key.get());
							}
						}
						reader.close();
					}
				}
			}
		}
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
	private static boolean isCompress = false;
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> _values, Context context) 
			throws IOException, InterruptedException{
		if(_key.sign != Config.SMALLSIGN){
			return;
		}
		//System.out.println(isCompress);
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
							context.write(new HVArray(_key.vertexArray), new HVArray(v1, v3, v4));
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
		resList = new TLongArrayList();
		isCompress = context.getConfiguration().getBoolean("result.compression", false);
	}
	
	@Override
	public void cleanup(Context context){
		resList.clear();
		resList = null;
	}
}

class EnumNear5CliqueCountReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, LongWritable> {

	// private static TLongArrayList triangleList = null;
	private static TLongHashSet triSet = null;
	private static boolean isCompress = false;

	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> _values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		long count = 0L;

		triSet.clear();
		long v1 = 0, v3 = 0, v4 = 0;
		for (HVArray value : _values) {
			if (_key.sign == Config.SMALLSIGN) {
				triSet.add(value.getFirst());
			} else {
				// If value.size() > 2, it is not possible to be not compressed
				if (value.size() > 2) { 
					long[] cliqueArray = value.toArrays();
					for (int j = 0; j < cliqueArray.length - 1; ++j) {
						for (int k = j + 1; k < cliqueArray.length; ++k) {
							count += triSet.size();
							if (triSet.contains(cliqueArray[j])) {
								count -= 1;
							}
							if (triSet.contains(cliqueArray[k])) {
								count -= 1;
							}
						}
					}

				} else {
					if (isCompress) {
						v3 = value.getFirst();
						v4 = value.getSecond();
						count += triSet.size();
						if (triSet.contains(v3)) {
							count -= 1;
						}
						if (triSet.contains(v4)) {
							count -= 1;
						}
					} else { // If not compress, touch every results instead
						TLongIterator iter = triSet.iterator();
						while(iter.hasNext()) {
							v1 = iter.next();
							v3 = value.getFirst();
							v4 = value.getSecond();
							if(v1 != v3 && v1 != v4) {
								count += 1;
							}
						}
					}
				}
			}
		}
		if(count > 0)
			context.write(NullWritable.get(), new LongWritable(count));
	}

	@Override
	public void setup(Context context) {
		triSet = new TLongHashSet();
		isCompress = context.getConfiguration().getBoolean(
				"result.compression", false);
	}

	@Override
	public void cleanup(Context context) {
		triSet.clear();
		triSet = null;
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
		//System.out.println(HyperVertex.HVArrayToString(array));
		triSet.clear();
		for(int i = 1; i < triSize + 1; ++i){
			triSet.add(array[i]);
		}

		//System.out.println("key: " + HyperVertex.HVArrayToString(_key.toArrays()));
		//System.out.println(HyperVertex.HVArrayToString(array));
		int i = triSize + 1;
		while (i < array.length) {
				// This is not a size
			if(array[i] < 0 || array[i] >= (long) 1 << 26) {
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
				long[] cliqueArray = Arrays.copyOfRange(array, i + 1, i + 1
						+ cliqueSize);
				for (int j = 0; j < cliqueArray.length - 1; ++j) {
					for (int k = j + 1; k < cliqueArray.length; ++k) {
						count += triSize;
						if (triSet.contains(cliqueArray[j])) {
							count -= 1;
						}
						if (triSet.contains(cliqueArray[k])) {
							count -= 1;
						}
					}
				}
				i += cliqueSize + 1;
			}
		}
		if(count > 0)
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