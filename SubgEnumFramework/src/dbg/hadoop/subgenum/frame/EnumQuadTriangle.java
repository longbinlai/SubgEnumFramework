package dbg.hadoop.subgenum.frame;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import dbg.hadoop.subgraphs.utils.BinarySearch;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.InputInfo;


@SuppressWarnings("deprecation")
public class EnumQuadTriangle{
	
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

		String[] opts = { workDir + "frame.quadtriangle.res.1", "", workDir + "frame.quadtriangle.res", 
				inputInfo.numReducers, inputInfo.jarFile };

		ToolRunner.run(conf, new GeneralDriver("Frame QuadTriangle",
				EnumQuadTriangleMapper.class, EnumQuadTriangleReducer.class,
				NullWritable.class,
				LongWritable.class, // OutputKV
				HVArray.class,
				HVArray.class, // MapOutputKV
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
				HVArrayComparator.class), opts);
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

class EnumQuadTriangleMapper extends
	Mapper<HVArray, HVArray, HVArray, HVArray> {
	
	@Override
	public void map(HVArray _key, HVArray _value,
			Context context) throws IOException, InterruptedException {
		
		long v1 = _key.getFirst();
		long v3 = _key.getSecond();
		
		long[] newArray = new long[_value.size() + 1];
		long[] oldArray = _value.toArrays();
		System.arraycopy(oldArray, 0, newArray, 1, _value.size());
		HVArray out = new HVArray(newArray);
		
		for(long v4 : oldArray){
			out.set(0, v3);
			context.write(new HVArray(v1, v4), out);
			out.set(0, v1);
			context.write(new HVArray(v3, v4), out);
		}
	}
}

class EnumQuadTriangleReducer extends
	Reducer<HVArray, HVArray, NullWritable, LongWritable> {
	
	@Override
	public void reduce(HVArray key, Iterable<HVArray> values, 
			Context context) throws IOException, InterruptedException{
		List<HVArray> mem = new ArrayList<HVArray>();
		for(HVArray val : values){
			mem.add(new HVArray(val));
		}
		int len = mem.size();
		long[] arr1 = null, arr2 = null;
		int n = 0, m = 0, c = 0;
		long u3 = 0, u5 = 0;
		long count = 0;
		for(int i = 0; i < len - 1; ++i){
			for(int j = i + 1; j < len; ++j){
				arr1 = mem.get(i).toArrays();
				arr2 = mem.get(j).toArrays();
				u3 = arr1[0];
				u5 = arr2[0];
				// Remove the pivot and the duplication
				n = arr1.length - 2;
				m = arr2.length - 2;
				
				if(BinarySearch.binarySearch(u5, arr1, 1, arr1.length) != -1) n -= 1;
				if(BinarySearch.binarySearch(u3, arr2, 1, arr2.length) != -1) m -= 1;
				
				c = numCommonElems(arr1, arr2) - 1;
				count += n * m - c;
			}
		}
		if(count != 0)
			context.write(NullWritable.get(), new LongWritable(count));
	}
	

	
	
	
	/**
	 * Count the common elements in two sorted arrays
	 * @param array1
	 * @param array2
	 * @return
	 */
	public static int numCommonElems(long[] array1, long[] array2){
		if(array1.length == 0 || array2.length == 0){
			return 0;
		}
		int len1 = array1.length, len2 = array2.length;
		// One array's smallest value is larger than the other's largeset one
		// it is not possible to have overlap.
		if(array1[1] > array2[len2 - 1] || array2[1] > array1[len1 - 1]){
			return 0;
		}
		int pos1 = 1, pos2 = 1;
		int result = 0;
		while(pos1 < len1 && pos2 < len2){
			if(array1[pos1] == array2[pos2]){
				++result;
				++pos1;
				++pos2;
			}
			else if(array1[pos1] < array2[pos2]){
				++pos1;
			}
			else{
				++pos2;
			}
		}
		return result;
	}
}
