package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArrayComparator;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArraySignComparator;
import dbg.hadoop.subgraphs.utils.BinarySearch;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.InputInfo;

@SuppressWarnings("deprecation")
public class EnumQuadTriangleLD{
	
	public static void run(InputInfo inputInfo) throws Exception{
		String workDir = inputInfo.workDir;
		
		Configuration conf = new Configuration();
		
		// First enumerate the chordal square
		if(!inputInfo.isChordalSquareSkip){
			inputInfo.isCountOnly = false;
			inputInfo.isResultCompression = true;
			inputInfo.enableBF = false;
			inputInfo.outputDir = "frame.quadtriangle.res.1";
			EnumChordalSquare.run(inputInfo);
		}

		String[] opts = { workDir + "frame.quadtriangle.res.1", workDir + "triangle.res",
				workDir + "frame.quadtriangle.res.2", 
				inputInfo.numReducers, inputInfo.jarFile };

		ToolRunner.run(conf, new GeneralDriver("Frame QuadTriangle",
				EnumQuadTriangleLDStage2Mapper1.class, 
				EnumQuadTriangleLDStage2Mapper2.class, 
				EnumQuadTriangleLDStage2Reducer.class,
				HVArray.class,
				HVArray.class, // OutputKV
				HVArraySign.class, // MapOutputKV
				HVArray.class,
				SequenceFileInputFormat.class, 
				SequenceFileInputFormat.class, 
				SequenceFileOutputFormat.class,
				HVArraySignComparator.class, HVArrayComparator.class), opts);
		
		opts[0] = workDir + "frame.quadtriangle.res.2";
		opts[2] = workDir + "frame.quadtriangle.res";
		
		ToolRunner.run(conf, new GeneralDriver("Frame QuadTriangle",
				EnumQuadTriangleLDStage3Mapper.class, 
				EnumQuadTriangleLDStage2Mapper2.class, 
				EnumQuadTriangleLDStage3Reducer.class,
				NullWritable.class,
				LongWritable.class, // OutputKV
				HVArraySign.class,
				HVArray.class, // MapOutputKV
				SequenceFileInputFormat.class, 
				SequenceFileInputFormat.class, 
				SequenceFileOutputFormat.class,
				HVArraySignComparator.class, HVArrayComparator.class), opts);
	}
	

	public static void countOnce(InputInfo inputInfo) throws Exception{
		if(inputInfo.isCountPatternOnce){
			String[] opts = { inputInfo.workDir + "frame.quadtriangle.res",
					inputInfo.workDir + "frame.quadtriangle.cnt", 
					inputInfo.numReducers, inputInfo.jarFile };
			ToolRunner.run(new Configuration(), new GeneralPatternCountDriver(
					GeneralPatternCountIdentityMapper.class), opts);
			
		}
	}
}

/**
 * Map the compressed chordal square for next join
 * @author robeen
 *
 */
class EnumQuadTriangleLDStage2Mapper1 extends
	Mapper<HVArray, HVArray, HVArraySign, HVArray>{
	
	@Override
	public void map(HVArray key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		long[] oldArray = value.toArrays();
		int len = oldArray.length;
		long[] newArray = new long[len + 2];
		long v1 = key.getFirst();
		long v3 = key.getSecond();
		
		System.arraycopy(oldArray, 0, newArray, 2, len);
		HVArray out = new HVArray(newArray);

		for (long v2 : oldArray) {
			out.set(0, v3);
			context.write(new HVArraySign(v1, v2, Config.LARGESIGN), out);
			out.set(0, v1);
			context.write(new HVArraySign(v3, v2, Config.LARGESIGN), out);
		}
	}
}

class EnumQuadTriangleLDStage2Mapper2 extends
	Mapper<NullWritable, HVArray, HVArraySign, HVArray>{
	
	@Override
	public void map(NullWritable key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		
		long v1 = value.getFirst();
		long v2 = value.getSecond();
		long v3 = value.getLast();
		
		context.write(new HVArraySign(v1, v2, Config.SMALLSIGN),  new HVArray(v3));
		context.write(new HVArraySign(v2, v1, Config.SMALLSIGN),  new HVArray(v3));
		context.write(new HVArraySign(v2, v3, Config.SMALLSIGN),  new HVArray(v1));
		context.write(new HVArraySign(v3, v2, Config.SMALLSIGN),  new HVArray(v1));
		context.write(new HVArraySign(v1, v3, Config.SMALLSIGN),  new HVArray(v2));
		context.write(new HVArraySign(v3, v1, Config.SMALLSIGN),  new HVArray(v2));
	}
}

class EnumQuadTriangleLDStage2Reducer extends
	Reducer<HVArraySign, HVArray, HVArray, HVArray>{
	
	private static final List<Long> heap = new ArrayList<Long>();
	@Override
	public void reduce(HVArraySign key, Iterable<HVArray> values, Context context)
			throws IOException, InterruptedException{
		if(key.sign != Config.SMALLSIGN){
			return;
		}
		heap.clear();
		long v5 = 0;
		
		for(HVArray value : values){
			if(key.sign == Config.SMALLSIGN){
				heap.add(value.getFirst());
			}
			else{
				Iterator<Long> iter = heap.iterator(); 
				while(iter.hasNext()){
					v5 = iter.next();
					if(value.getFirst() < v5){
						value.set(1, v5);
						//System.out.println("out-key : " + HyperVertex.HVArrayToString(key.vertexArray.toArrays()));
						//System.out.println("out-value : " + HyperVertex.HVArrayToString(value.toArrays()));
						context.write(key.vertexArray, value);
					}
				}
			}
		}
	}
}


class EnumQuadTriangleLDStage3Mapper extends
	Mapper<HVArray, HVArray, HVArraySign, HVArray>{
	
	@Override
	public void map(HVArray key, HVArray value, Context context) 
			throws IOException, InterruptedException{
		
		long v1 = key.getFirst();
		long v4 = key.getSecond();
		long v5 = value.getSecond();
		
		value.set(1, v4);
		context.write(new HVArraySign(v1, v5, Config.LARGESIGN), value);
	}
}


class EnumQuadTriangleLDStage3Reducer extends
	Reducer<HVArraySign, HVArray, NullWritable, LongWritable>{
	
	private static final List<Long> heap = new ArrayList<Long>();
	@Override
	public void reduce(HVArraySign key, Iterable<HVArray> values, Context context)
			throws IOException, InterruptedException{
		if(key.sign != Config.SMALLSIGN){
			return;
		}
		heap.clear();
		long v6 = 0, v4 = 0, v3 = 0;
		long v5 = key.vertexArray.getSecond();
		long count = 0;
		//int size = 0;
		long[] array = null;
		for(HVArray value : values){
			if(key.sign == Config.SMALLSIGN){
				heap.add(value.getFirst());
			}
			else{
				array = value.toArrays();
				v3 = array[0];
				v4 = array[1];
				Iterator<Long> iter = heap.iterator(); 
				while(iter.hasNext()){
					v6 = iter.next();
					if(v4 == v6 || v3 == v6) continue;
					for(int i = 2; i < array.length; ++i){
						if(array[i] != v5 && array[i] != v6 && array[i] != v4){
							count += 1;
						}
					}
				}	
			}
		}
		if(count != 0){
			context.write(NullWritable.get(), new LongWritable(count));
		}
	}
	
}



