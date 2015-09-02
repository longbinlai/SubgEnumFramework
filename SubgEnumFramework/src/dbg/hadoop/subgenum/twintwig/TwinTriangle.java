package dbg.hadoop.subgenum.twintwig;

import java.io.IOException;
import java.net.URI;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

import dbg.hadoop.subgenum.frame.GeneralDriver;
import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayGroupComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.Utility;
import dbg.hadoop.subgraphs.utils.InputInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

@SuppressWarnings("deprecation")
public class TwinTriangle {
	
	public static void main(String[] args) throws Exception {
		run(new InputInfo(args));
	}
	
	public static void run(InputInfo inputInfo) throws Exception{
		String inputFilePath = inputInfo.inputFilePath;
		float falsePositive = inputInfo.falsePositive;
		boolean enableBF = inputInfo.enableBF;
		int maxSize = inputInfo.maxSize;
		String workDir = inputInfo.workDir;
		
		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		if (workDir.toLowerCase().contains("hdfs")) {
			int pos = workDir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(workDir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		String stageOneOutput = workDir + "tt.twintriangle.tmp.1";
		String stageTwoOutput = workDir + "tt.twintriangle.res";
		
		if(Utility.getFS().isDirectory(new Path(stageOneOutput)))
			Utility.getFS().delete(new Path(stageOneOutput));
		if(Utility.getFS().isDirectory(new Path(stageTwoOutput)))
			Utility.getFS().delete(new Path(stageTwoOutput));
		
		Configuration conf = new Configuration();
		conf.setBoolean("enable.bloom.filter", enableBF);
		if(enableBF){
			conf.setFloat("bloom.filter.false.positive.rate", falsePositive);
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
				.toString() + "/" + Config.bloomFilterFileDir + "/" + "bloomFilter." + 
					Config.EDGE + "." + falsePositive), conf);
		}
		
		String[] opts = { workDir + "adjList2." + maxSize, "", stageOneOutput,
				inputInfo.numReducers, inputInfo.jarFile};
		
		ToolRunner.run(conf, new GeneralDriver("TwinTwig TwinTriangle Stage One", 
				TwinTriangleStageOneMapper.class, null,
				TwinTriangleStageOneReducer.class, 
			    NullWritable.class, HVArray.class, //OutputKV
				HVArraySign.class, LongWritable.class, //MapOutputKV
				SequenceFileInputFormat.class, null,
				SequenceFileOutputFormat.class, 
				HVArraySignComparator.class, HVArrayGroupComparator.class), opts);
		
		String[] opts1 = { workDir + "adjList2." + maxSize, stageOneOutput, stageTwoOutput,
				inputInfo.numReducers, inputInfo.jarFile };
		ToolRunner.run(conf, new GeneralDriver("TwinTwig TwinTriangle Stage Two", 
				TwinTriangleStageTwoMapper1.class, 
				TwinTriangleStageTwoMapper2.class,
				TwinTriangleStageTwoReducer.class, 
			    NullWritable.class, HVArray.class, //OutputKV
				HVArraySign.class, HVArray.class, //MapOutputKV
				SequenceFileInputFormat.class, 
				SequenceFileInputFormat.class,
				SequenceFileOutputFormat.class, 
				HVArraySignComparator.class, HVArrayGroupComparator.class), opts1);
		
		Utility.getFS().delete(new Path(stageOneOutput));
		Utility.getFS().delete(new Path(stageTwoOutput));
	}
}

class TwinTriangleStageOneMapper extends
		Mapper<LongWritable, HyperVertexAdjList, HVArraySign, LongWritable> {

	private static boolean enableBF = true;
	private static BloomFilterOpr bloomfilterOpr = null;

	@Override
	public void map(LongWritable _key, HyperVertexAdjList _value,
			Context context) throws IOException, InterruptedException {

		long[] largerThanThis = _value.getLargeDegreeVertices();
		long[] smallerThanThisG0 = _value.getSmallDegreeVerticesGroup0();
		long[] smallerThanThisG1 = _value.getSmallDegreeVerticesGroup1();
		boolean isOutput = true;

		if (_value.isFirstAdd()) {
			// Generate TwinTwig 1
			for (int i = 0; i < largerThanThis.length - 1; ++i) {
				for (int j = i + 1; j < largerThanThis.length; ++j) {
					context.write(new HVArraySign(largerThanThis[i], _key.get(), Config.LARGESIGN), 
							new LongWritable(largerThanThis[j]));
				}
			}
		}

		for (int i = 0; i < smallerThanThisG1.length; ++i) {
			// Generate TwinTwig 3
			for (int k = i + 1; k < smallerThanThisG1.length; ++k) {
				if (enableBF) {
					isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(smallerThanThisG1[i]),
							HyperVertex.VertexID(smallerThanThisG1[k]));
				}
				if (isOutput) {
					context.write(new HVArraySign(smallerThanThisG1[i], smallerThanThisG1[k],
							Config.SMALLSIGN), _key);
					context.write(new HVArraySign(smallerThanThisG1[k], smallerThanThisG1[i],
							Config.SMALLSIGN), _key);
				}
				context.write(new HVArraySign(smallerThanThisG1[i], _key.get(), Config.LARGESIGN), 
						new LongWritable(smallerThanThisG1[k]));
			}
			// Generate TwinTwig 2
			for (int j = 0; j < largerThanThis.length; ++j) {
				if (enableBF) {
					isOutput = bloomfilterOpr.get().test(
							HyperVertex.VertexID(smallerThanThisG1[i]),
							HyperVertex.VertexID(largerThanThis[j]));
				}
				if (isOutput) {
					context.write(new HVArraySign(smallerThanThisG1[i], largerThanThis[j],
							Config.SMALLSIGN), _key);
				}
				context.write(new HVArraySign(smallerThanThisG1[i], _key.get(),
							Config.LARGESIGN), new LongWritable(largerThanThis[j]));
			}
		}

		if (smallerThanThisG0.length > 0) {
			for (int i = 0; i < smallerThanThisG0.length; ++i) {
				for (int j = 0; j < smallerThanThisG1.length; ++j) {
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
								HyperVertex.VertexID(smallerThanThisG0[i]),
								HyperVertex.VertexID(smallerThanThisG1[j]));
					}
					if (isOutput) {
						context.write(new HVArraySign(smallerThanThisG0[i], smallerThanThisG1[j],
								Config.SMALLSIGN), _key);
						context.write(new HVArraySign(smallerThanThisG1[j], smallerThanThisG0[i],
								Config.SMALLSIGN), _key);
					}
					context.write(new HVArraySign(smallerThanThisG0[i], _key.get(),
							Config.LARGESIGN), new LongWritable(smallerThanThisG1[j]));
				}
			}
		}
	}

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		if (enableBF && bloomfilterOpr == null) {
			bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
					"bloom.filter.false.positive.rate", (float) 0.001),
					Config.EDGE);
			try {
				bloomfilterOpr.obtainBloomFilter(conf);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class TwinTriangleStageOneReducer extends
		Reducer<HVArraySign, LongWritable, NullWritable, HVArray> {

	private static TLongArrayList list = null;

	@Override
	public void reduce(HVArraySign _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		list.clear();
		long v0 = _key.vertexArray.getSecond();
		long v1 = _key.vertexArray.getFirst();

		for (LongWritable val : values) {
			if (_key.sign == Config.SMALLSIGN) {
				list.add(val.get());
			} else {
				TLongIterator iter = list.iterator();
				long v4 = val.get();
				while (iter.hasNext()) {
					long v2 = iter.next();
					if(v2 != v4){
						long[] array = { v0, v1, v2, v4};
						context.write(NullWritable.get(), new HVArray(array));
					}
				}
			}
		}

	}

	@Override
	public void setup(Context context) {
		list = new TLongArrayList();
	}

	@Override
	public void cleanup(Context context) {
		list.clear();
		list = null;
	}
}

class TwinTriangleStageTwoMapper1 extends
	Mapper<LongWritable, HyperVertexAdjList, HVArraySign, HVArray> {

	private static boolean enableBF = true;
	private static BloomFilterOpr bloomfilterOpr = null;
	
	@Override
	public void map(LongWritable _key, HyperVertexAdjList _value,
			Context context) throws IOException, InterruptedException {

		long[] largerThanThis = _value.getLargeDegreeVertices();
		//long[] smallerThanThisG0 = _value.getSmallDegreeVerticesGroup0();
		long[] smallerThanThisG1 = _value.getSmallDegreeVerticesGroup1();
		boolean isOutput = true;
		if (_value.isFirstAdd()) {
			// Generate TwinTwig 1
			for (int i = 0; i < largerThanThis.length - 1; ++i) {
				for (int j = i + 1; j < largerThanThis.length; ++j) {
					if(enableBF){
						isOutput = bloomfilterOpr.get().test(HyperVertex.VertexID(largerThanThis[i]), 
								HyperVertex.VertexID(largerThanThis[j]));
					}
					if(isOutput) {
						context.write(new HVArraySign(largerThanThis[i], largerThanThis[j], Config.SMALLSIGN), 
							new HVArray(_key.get()));
						context.write(new HVArraySign(largerThanThis[j], largerThanThis[i], Config.SMALLSIGN), 
							new HVArray(_key.get()));
					}
				}
			}
			
			for (int i = 0; i < smallerThanThisG1.length; ++i) {
				for (int j = 0; j < largerThanThis.length; ++j) {
					if (enableBF) {
						isOutput = bloomfilterOpr.get().test(
								HyperVertex.VertexID(smallerThanThisG1[i]),
								HyperVertex.VertexID(largerThanThis[j]));
					}
					if (isOutput) {
						context.write(new HVArraySign(smallerThanThisG1[i], largerThanThis[j],
								Config.SMALLSIGN), new HVArray(_key.get()));
					}
				}
			}
		}
	}
	
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		// We use bloomfilter as static. If it is already loaded, we will have
		// bloomfilterOpr != null, and we donot load it again in the case.
		enableBF = conf.getBoolean("enable.bloom.filter", false);
		if (enableBF && bloomfilterOpr == null) {
			bloomfilterOpr = new BloomFilterOpr(conf.getFloat(
					"bloom.filter.false.positive.rate", (float) 0.001),
					Config.EDGE);
			try {
				bloomfilterOpr.obtainBloomFilter(conf);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class TwinTriangleStageTwoMapper2 extends
		Mapper<NullWritable, HVArray, HVArraySign, HVArray> {

	@Override
	public void map(NullWritable _key, HVArray _value, Context context)
			throws IOException, InterruptedException {
		context.write(
				new HVArraySign(_value.get(0), _value.get(3), Config.LARGESIGN),
				new HVArray(_value.get(1), _value.get(2)));
	}
}



class TwinTriangleStageTwoReducer extends
		Reducer<HVArraySign, HVArray, NullWritable, HVArray> {
	
	private static TLongArrayList list = null;
	
	@Override
	public void reduce(HVArraySign _key, Iterable<HVArray> values,
			Context context) throws IOException, InterruptedException {
		if (_key.sign != Config.SMALLSIGN) {
			return;
		}
		long v0 = _key.vertexArray.getFirst();
		long v4 = _key.vertexArray.getSecond();
		
		list.clear();
		
		for (HVArray val : values) {
			if (_key.sign == Config.SMALLSIGN) {
				list.add(val.getFirst());
			} else {
				long v1 = val.getFirst();
				long v2 = val.getSecond();
				TLongIterator iter = list.iterator();
				while(iter.hasNext()){
					long v3 = iter.next();
					if(v1 != v3 && v2 != v3 && v1 < v3){
						long[] array = { v0, v1, v2, v3, v4 };
						context.write(NullWritable.get(), new HVArray(array));
					}
				}
			}
		}
	}
	
	@Override
	public void setup(Context context) {
		list = new TLongArrayList();
	}

	@Override
	public void cleanup(Context context) {
		list.clear();
		list = null;
	}
}


