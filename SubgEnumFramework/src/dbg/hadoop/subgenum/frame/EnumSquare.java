package dbg.hadoop.subgenum.frame;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BinarySearch;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

@SuppressWarnings("deprecation")
public class EnumSquare {

	public static void run(InputInfo inputInfo) throws Exception {
		String workDir = inputInfo.workDir;
		int maxSize = inputInfo.maxSize;

		if(!Utility.getFS().isDirectory(new Path(workDir + "nonsmallneigh"))){
			String[] opts0 = { workDir + "adjList2.0", "", workDir + "nonsmallneigh",
					inputInfo.numReducers, inputInfo.jarFile};
			ToolRunner.run(new Configuration(), new GeneralDriver("CalNonSmall", 
					CalNonSmallMapper.class, 
					CalNonSmallReducer.class, 
					NullWritable.class, LongWritable.class, //OutputKV
					//HVArray.class, LongWritable.class, //MapOutputKV
					SequenceFileInputFormat.class, 
					SequenceFileOutputFormat.class,
					null), opts0);
		}
		
		Configuration conf = new Configuration();
		
		FileStatus[] files = Utility.getFS().listStatus(new Path(workDir + "nonsmallneigh"));
		for(FileStatus f : files){
			DistributedCache.addCacheFile(f.getPath().toUri(), conf);
		}
		//DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + "/nonsmallneigh"), conf);
		
		conf.setInt("mapred.input.max.size", maxSize);
		conf.setBoolean("enable.bloom.filter", inputInfo.enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", inputInfo.falsePositive);
		if(inputInfo.enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.TWINTWIG1 + "." + inputInfo.falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + "/" +
					Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String[] opts = { workDir + "adjList2." + maxSize, "",
				workDir + "frame.square.res",  inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new GeneralDriver("Frame Square", 
						EnumSquareMapper.class, 
						EnumSquareReducer.class, 
						HVArray.class, HVArray.class, //OutputKV
						HVArray.class, LongWritable.class, //MapOutputKV
						SequenceFileInputFormat.class, 
						SequenceFileOutputFormat.class,
						HVArrayComparator.class), opts);	
	}
	
	public static void countOnce(InputInfo inputInfo) throws Exception{
		if (inputInfo.isCountPatternOnce) {
			String[] opts2 = { inputInfo.workDir + "frame.square.res",
					inputInfo.workDir + "frame.square.cnt",
					inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					SquareCountMapper.class), opts2);
		}
	}
	
}

class CalNonSmallMapper extends
		Mapper<LongWritable, HyperVertexAdjList, NullWritable, LongWritable> {
	public void map(LongWritable _key, HyperVertexAdjList _value,
			Context context) throws IOException, InterruptedException {
		if (_value.getSmallNum() == 0)
			context.write(NullWritable.get(), _key);

	}
}

class CalNonSmallReducer extends
		Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
	@Override
	public void reduce(NullWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		for (LongWritable val : values) {
			context.write(NullWritable.get(), val);
		}
	}
}

@SuppressWarnings("deprecation")
class EnumSquareMapper extends Mapper<LongWritable, HyperVertexAdjList, HVArray, LongWritable> {
	private static TLongHashSet invalidNodeSet = null;
	private static boolean enableBF = false;
	private static BloomFilterOpr bloomfilterOpr = null;
	private static int maxSize = 0;
	
	public void map(LongWritable _key, HyperVertexAdjList _value, Context context) throws IOException, InterruptedException{
		if (maxSize == 0) {
			long[] neighbors = _value.getNeighbors();
			TLongArrayList validNbrs = new TLongArrayList();
			for (long v : neighbors) {
				if (!invalidNodeSet.contains(v)) {
						validNbrs.add(v);
				}
			}
			handleOutput(validNbrs.toArray(), null, _key, context);
		}
		else{
			TLongArrayList validNbrs1 = new TLongArrayList();
			TLongArrayList validNbrs2 = new TLongArrayList();
			if(!_value.existBackup()){
				for (long v : _value.getSmallDegreeVerticesGroup1()) {
					if (!invalidNodeSet.contains(v)) {
						validNbrs1.add(v);
					}
				}
				for (long v : _value.getLargeDegreeVertices()) {
					if (!invalidNodeSet.contains(v)) {
						validNbrs2.add(v);
					}
				}
			} else {
				for (long v : _value.getSmallDegreeVerticesGroup0()) {
					if (!invalidNodeSet.contains(v)) {
						validNbrs1.add(v);
					}
				}
				for (long v : _value.getSmallDegreeVerticesGroup1()) {
					if (!invalidNodeSet.contains(v)) {
						validNbrs2.add(v);
					}
				}
			}
			if(!_value.existBackup()){
				if(_value.isFirstAdd()){
					handleOutput(validNbrs2.toArray(), null, _key, context);
				}
				handleOutput(validNbrs1.toArray(), null, _key, context);
			}
			handleOutput(validNbrs1.toArray(), validNbrs2.toArray(), _key, context);
		}
	}
	
	private void handleOutput(long[] array1, long[] array2, LongWritable _key, Context context) 
			throws IOException, InterruptedException{
		boolean isOutput = true;
		if(array2 == null){
			for(int i = 0; i < array1.length - 1; ++i){
				for(int j = i + 1; j < array1.length; ++j){
					long v1 = array1[i];
					long v2 = array1[j];
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
								HyperVertex.VertexID(v1), HyperVertex.VertexID(v2));
					}
					if(isOutput){
						context.write(new HVArray(v1, v2), _key);
					}
				}
			}
		}
		else{
			for(int i = 0; i < array1.length; ++i){
				for(int j = 0; j < array2.length; ++j){
					long v1 = array1[i];
					long v2 = array2[j];
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
								HyperVertex.VertexID(v1), HyperVertex.VertexID(v2));
					}
					if(isOutput){
						context.write(new HVArray(v1, v2), _key);
					}
				}
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		maxSize = conf.getInt("mapred.input.max.size", 0);
		LocalFileSystem fs = new LocalFileSystem();

		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		try {
			if (enableBF && bloomfilterOpr == null) {
				bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
						"bloom.filter.false.positive.rate", (float) 0.001), Config.TWINTWIG1);
				bloomfilterOpr.obtainBloomFilter(conf);
			}

			// Read the invalid nodes
			if (invalidNodeSet == null) {
				invalidNodeSet = new TLongHashSet();
				for (int i = 0; i < paths.length; ++i) {
					if (paths[i].toString().contains("part-r-")) {
						SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[i], conf);
						NullWritable key = null;
						LongWritable val = null;
						while (reader.next(key, val)) {
							invalidNodeSet.add(val.get());
						}
						reader.close();
					}
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}

class EnumSquareReducer extends	Reducer<HVArray,LongWritable, HVArray, HVArray> {
	private static TLongArrayList list = null;
	
	@Override
	public void reduce(HVArray _key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException{
		list.clear();
		for (LongWritable val : values) {
			list.add(val.get());
		}
		list.sort();
		context.write(_key, new HVArray(list.toArray()));
	}
	
	@Override
	public void setup(Context context){
		list = new TLongArrayList();
	}
	
	@Override
	public void cleanup(Context context){
		list.clear();
		list = null;
	}
}

class SquareCountMapper extends
		Mapper<HVArray, HVArray, NullWritable, LongWritable> {
	@Override
	public void map(HVArray _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		long count = 0L;
		int largeThanMinIndex = BinarySearch.findLargeIndex(_key.getFirst(), _value.toArrays());
		count = (2 * _value.size() - 1 - largeThanMinIndex) * largeThanMinIndex / 2;
		context.write(NullWritable.get(), new LongWritable(count));
	}
}





