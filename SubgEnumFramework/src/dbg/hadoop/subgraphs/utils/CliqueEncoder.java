package dbg.hadoop.subgraphs.utils;

import java.util.Arrays;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

/**
 * 
 * The cliques w.r.t. v has v as the minimum vertex and the vertices are all v's neighbors.
 * Another thing worth knowing is that if there are a large clique K containing v, any cliqueSize - 1
 * vertices in K with v form a clique. There, we just use the large clique to record all these cliques.
 * As the result, the Clique Encoding has the form:
 * (v, cliqueSize - 1, sizeOfVerticesIn{K}, verticesIn{K}, cliqueArray), 
 * where K is the large clique that contains v
 */
public class CliqueEncoder{
  private int k = 4;
  private long v = 0;
  private int cliqueSetSize = -1;
  private TLongHashSet cliqueSet;
  private TLongArrayList normalBuffer;
  
  public CliqueEncoder(long curV, int _k){
	  k = _k;
	  v = curV;
	  cliqueSet = new TLongHashSet();
	  normalBuffer = new TLongArrayList();
  }
  
  public CliqueEncoder(long curV, int _k, int size){
	  k = _k;
	  v = curV;
	  cliqueSetSize = 0;
	  cliqueSet = new TLongHashSet();
	  normalBuffer = new TLongArrayList();
  }
 
 
  
  public void addCliqueVertex(long[] vertices){
    if(vertices == null){
      return;
    }
    for(int i = 0; i < vertices.length; ++i){
      if(!cliqueSet.contains(vertices[i])){
        cliqueSet.add(vertices[i]);
      }
    }
  }
  
  public void addNormalVertices(long[] vertices){
    if(vertices == null){
      return;
    }
    normalBuffer.addAll(vertices);
  }
  
  public void addNormalVerticesWithCompress(long[] vertices, int k, long[] commonCliqueNeighbors) {
	  
	  int len = 0;
	  len = commonCliqueNeighbors == null ? vertices.length + 1 :
		  		vertices.length + commonCliqueNeighbors.length + 1;
	  normalBuffer.add(len);
	  normalBuffer.add(k);
	  normalBuffer.addAll(vertices);
	  if(commonCliqueNeighbors != null) {
		  Arrays.sort(commonCliqueNeighbors);
		  normalBuffer.addAll(commonCliqueNeighbors);
	  }
  }
  
  public long[] getEncodedCliques() {
    TLongArrayList buffer = new TLongArrayList(cliqueSet.size() + normalBuffer.size() + 3);
    buffer.add(v);
    buffer.add((long)k);
    if((cliqueSetSize == -1 || cliqueSet.size() == 0)
    		&& normalBuffer.size() == 0) {
    	return null;
    }
    if(cliqueSet.size() != 0){
    	buffer.add(cliqueSet.size());
    }
    else {
    	buffer.add(cliqueSetSize);
    }
    long[] array = cliqueSet.toArray();
    Arrays.sort(array);
    buffer.addAll(array);
    buffer.addAll(normalBuffer.toArray());
    return buffer.toArray();
  }
  
	public static long getNumCliquesFromEncodedArray(long[] array) {
		long res = 0L;
		assert (array != null && array.length >= 3);
		int sizeOfClique = (int) array[1];
		//int sizeOfVerticesInLargeClique = (int) array[2];
		//res += binorm(sizeOfVerticesInLargeClique, sizeOfClique);
		res += (array.length - 3) / sizeOfClique;
		return res;
	}
	
	public static long getNumCliquesFromEncodedArrayV2(long[] array) {
		long res = 0L;
		assert (array != null && array.length >= 3);
		int sizeOfClique = (int) array[1];
		int sizeOfVerticesInLargeClique = (int) array[2];
		if(sizeOfVerticesInLargeClique == -1) {
			return getNumCliquesFromEncodedArray(array);
		}
		
		res += binorm(sizeOfVerticesInLargeClique, sizeOfClique);
		int index = 3 + sizeOfVerticesInLargeClique;
		while(index < array.length){
			int len = (int) array[index];
			int nonCliqueVerticesSize = (int) array[index + 1];
			if(len - 1 == nonCliqueVerticesSize && nonCliqueVerticesSize == sizeOfClique){
				res += 1;
			}
			else {
				res += binorm(len - 1 - nonCliqueVerticesSize, sizeOfClique - nonCliqueVerticesSize);
			}
			index += len + 1;
		}
		return res;
	}
	
	public void clear(){
		this.cliqueSet.clear();
		this.normalBuffer.clear();
	}

	public static long binorm(int n, int d) {
		if (n == 0 || n < d) {
			return 0;
		}
		long res = 1;
		if(d == 0 || n == d){
			return res;
		}
		for (int i = 0; i < d; ++i) {
			res *= (n - i);
		}
		for (int i = 1; i <= d; ++i) {
			res /= i;
		}
		return res;
	}
}