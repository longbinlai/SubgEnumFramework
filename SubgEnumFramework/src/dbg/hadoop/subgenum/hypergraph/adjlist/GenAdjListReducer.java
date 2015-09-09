package dbg.hadoop.subgenum.hypergraph.adjlist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;


//public class InitReducer extends MapReduceBase implements

/**
 * There are several types of adjlist.
 * Generally, a typical adjList should be key {smallDegreeVertices} {largeDegreeVertices}.
 * However, where {smallDegreeVertices} is of size too large, we have to partition it into multiple groups,
 * that says group0, group1, group2, ..... In this case, the adjList are of several types.
 * 1. No partition: key {empty} {smallDegreeVertices} {largeDegreeVertices}.
 * 2. Partition: 
 * 2.1 key {empty} {groupi} {largeDegreeVertices} 
 * 2.2 key {groupi} {groupj} (for i < j) {empty}.
 * In the partition situation, as we can see, in order to generate twintwig2, largeDegreeVertices 
 * would be copied to multiple groups, which will cause duplication when generate twintwig1.
 * Hence, we need to make a special insertion when largeDegreeVertices is first added, which is
 * to make array[0] = -1 for the first insertion. 
 * @author robeen
 *
 */
public class GenAdjListReducer extends
		Reducer<LongWritable, LongWritable, LongWritable, HyperVertexAdjList> {
	//private MaxHeapLimitSize heaps = null;
	private HyperVertexHeap largeDegreeVertices = null;
	private HyperVertexHeap smallDegreeVertices = null;
	private List<long[]> list = new ArrayList<long[]>();
	private static int maxSize = 0;

	@Override
	public void reduce(LongWritable _key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException{
		largeDegreeVertices.clear();
		smallDegreeVertices.clear();
		
		//System.out.println(maxSize);
		
		list.clear();
		boolean outputIsoVertex = false;
		for(LongWritable value: values){
			if(value.get() == -1 || value.get() == _key.get()){
				outputIsoVertex = true;
				continue;
			}
			if (HyperVertex.Degree(value.get()) > 1) { // Ignore vertices with deg = 1
				if (HyperVertex.compare(_key.get(), value.get()) < 0) {
					largeDegreeVertices.insert(value.get());
				} else {
					smallDegreeVertices.insert(value.get());
				}
			}
		}
		if(outputIsoVertex && largeDegreeVertices.isEmpty() && smallDegreeVertices.isEmpty()){
			context.write(_key, new HyperVertexAdjList());
			return;
		}
		largeDegreeVertices.sort();
		smallDegreeVertices.sort();
		
		try {
			partitionSmallDegreeVertices(maxSize);
			int size = list.size();
			
			if (size == 0) {
				context.write(_key, new HyperVertexAdjList(largeDegreeVertices));
			}
			else if(size == 1){ // Make it a special case to boost the operation, no partition case
				context.write(_key, 
						new HyperVertexAdjList(list.get(0), largeDegreeVertices, true));
			}
			else{
				for (int i = 0; i < size; ++i) {
					if(i == 0){
						context.write(_key,
							new HyperVertexAdjList(list.get(i), largeDegreeVertices, true));
					}
					else{
						context.write(_key, 
							new HyperVertexAdjList(list.get(i), largeDegreeVertices, false));
					}
					for (int j = i + 1; j < size; ++j) {
						context.write(_key, new HyperVertexAdjList(list.get(i), list.get(j)));
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private void partitionSmallDegreeVertices(int maxSize) throws Exception{
		list.clear();
		if(maxSize == 0 || maxSize > smallDegreeVertices.size()){
			list.add(smallDegreeVertices.getPartialArrays(0, smallDegreeVertices.size()));
			return;
		}
		int numGroups = smallDegreeVertices.size() / maxSize;
		if(smallDegreeVertices.size() - numGroups * maxSize > maxSize * Config.overSizeRate){
			if(numGroups != 0){
				numGroups += 1;
			}
		}
		
		int from = 0, to = 0;
		for(int i = 0; i < numGroups; ++i){
			from = i * maxSize;
			to = (i == numGroups - 1) ? smallDegreeVertices.size() : (i + 1) * maxSize;
			try {
				list.add(smallDegreeVertices.getPartialArrays(from, to));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		maxSize = conf.getInt("map.input.max.size", 0);
		smallDegreeVertices = new HyperVertexHeap(Config.HEAPINITSIZE);
		largeDegreeVertices = new HyperVertexHeap(Config.HEAPINITSIZE);
		//rand = new Random(System.currentTimeMillis());
	}
	
	@Override
	public void cleanup(Context context){
		list.clear();
		smallDegreeVertices.clear();
		largeDegreeVertices.clear();
	}
}