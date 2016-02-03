package dbg.hadoop.subgraphs.utils;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;

//import org.apache.log4j.Logger;

public class Graph{
	//private static Logger log = Logger.getLogger(Graph.class);
	private long unorientedSize=0;
	private long orientedSize=0;
	//private TLongLongHashMap cliqueMap = null;
	private TLongHashSet cliqueSet = null;
	private long countRunning = 0L;
	private CliqueEncoder encoder = null;

	//HashMap<String, HashSet<String>> graph = new HashMap<String, HashSet<String>>();  
	//HashMap<String, Integer> degrees =new HashMap<String, Integer>();
	
	HashMap<Long, TLongHashSet> graph = null;
	// This is the degree of current induced subgraphs.
	// The original degree is encapsulated with the long-vertex index
	TLongIntHashMap degrees = null;
	
	public Graph() {
		graph = new HashMap<Long, TLongHashSet>();
		degrees = new TLongIntHashMap();
		cliqueSet = new TLongHashSet();
	}

	public class GraphNodeComparator implements Comparator<Long> {
		private TLongIntHashMap deg;
		GraphNodeComparator(TLongIntHashMap degree){
			this.deg = degree;
		}

		@Override
		public int compare(Long a, Long b) {
			// TODO Auto-generated method stub
			int cmp = 0;
			int degA = this.deg.contains(a) ? this.deg.get(a) : 0;
			int degB = this.deg.contains(b) ? this.deg.get(b) : 0;

			// We want larger degree vertices stay on top of the queue
			cmp = (degA == degB) ? 0 : ((degA < degB) ? -1 : 1);
			if(cmp != 0){
				return cmp;
			}
			else{
				return (a < b ? -1 : 1);
			}
		}

	}
	
	public int getNodesNumber() {
		return this.graph.keySet().size();
	}
	
	public TLongArrayList getNodeList() {
		TLongArrayList l = new TLongArrayList();
		Iterator<Long> iter = this.graph.keySet().iterator();
		while(iter.hasNext()){
			l.add(iter.next());
		}
		return l; 
	}
	
	public TLongArrayList getNonCliqueNodeList() {
		TLongArrayList l = new TLongArrayList();
		Iterator<Long> iter = this.graph.keySet().iterator();
		while(iter.hasNext()){
			long a = iter.next();
			if(!cliqueSet.contains(a))
				l.add(a);
		}
		return l; 
	}

	public TLongArrayList getLargerNeighbors(long a) {
		TLongArrayList filtered = new TLongArrayList();
		long[] neigh = this.getSortedAdjList(a).toArray();
		int index = BinarySearch.findLargeIndex(a, neigh);
		filtered.add(neigh, index, neigh.length - index);
		filtered.sort();
		return filtered;
	}
	
	public TLongArrayList getNonCliqueLargerNeighbors(long a) {
		TLongArrayList filtered = new TLongArrayList();
		long[] neigh = this.getSortedAdjList(a).toArray();
		int index = BinarySearch.findLargeIndex(a, neigh);
		for(int i = index; i < neigh.length; ++i){
			if(!this.cliqueSet.contains(neigh[i]))
				filtered.add(neigh[i]);
		}
		return filtered;
	}

	private boolean checkPreviousAreAdjacent(TLongArrayList nodes, 
			int [] indexes, int upTo) {
		for (int i = 0; i< upTo; ++i) {
			if(!this.hasNeighbor(nodes.get(indexes[i]),  nodes.get(indexes[upTo]))) 
				return false;
		}
		return true;
	}

	private void addCommonCliqueNeighbors(long curVertex, TLongArrayList commonNeighbors) {
		TLongArrayList curNeighbors = this.getSortedAdjList(curVertex);
		TLongIterator iter = curNeighbors.iterator();
		while(iter.hasNext()) {
			long v = iter.next();
			if(this.cliqueSet.contains(v)){
				commonNeighbors.add(v);
			}
		}
	}
	
	private boolean checkFormClique(long[] curClique, long newNode) {
		for(long node : curClique) {
			if(!this.hasNeighbor(node, newNode)) return false;
		}
		return true;
	}
	
	public long countTriangles () {
		TLongArrayList l = this.getNodeList();
		TLongArrayList neighbors; 

		long countRunning=0;
		TLongIterator it = l.iterator();		

		long a,b,c;

		while (it.hasNext()) {
			a = it.next();
			neighbors = this.getLargerNeighbors(a);
			for (int i = 0; i < neighbors.size(); i++) {
				b = neighbors.get(i);
				for (int j = i + 1; j < neighbors.size(); j++) {
					c = neighbors.get(j);
					if (this.hasNeighbor(b, c))
						countRunning++;
				}
			}
		}
		return countRunning;
	}
	
	@Deprecated
	public long countCliquesOfSize(int cliqueSize) {
		TLongArrayList l = this.getNodeList();
		TLongArrayList neighbors; 

		countRunning=0;
		TLongIterator it = l.iterator();		

		int [] indexes = new int[cliqueSize-1];
		long a;
		long start = 0, end = 0, total = 0;
		while(it.hasNext()){
			a=it.next();
			neighbors = this.getLargerNeighbors(a);
			if(neighbors.size() >= cliqueSize-1) {
				indexes[0] = 0;
				int fixing = 0;
				boolean failure = false;
				while (fixing >= 0) {
					while (!failure) {
						while(!failure && !this.checkPreviousAreAdjacent(neighbors, indexes, fixing)){
							indexes[fixing]++;
							if (!(indexes[fixing]<(neighbors.size()-(indexes.length  - fixing - 1)))) {
								failure=true;

							}
						}
						if(!failure) {
							if (fixing+1<indexes.length) {
								fixing++;
								indexes[fixing] = indexes[fixing-1]+1;
							} else {
								countRunning++;
								indexes[fixing]++;
							}
							if (!(indexes[fixing]<(neighbors.size()-(indexes.length  - fixing - 1)))) {
								failure=true;
							}
						}
					}
					fixing--;
					if(fixing>=0) {
						indexes[fixing]++;
						failure=(!(indexes[fixing]<(neighbors.size()-(indexes.length  - fixing - 1))));
					}
				}
			}
		}
		return countRunning;
	}
	
	/**
	   * Output the clique instance rather than just count.
	   * The clique has curV as the minimum vertex, and when 
	   * finding k-clique, we simply find (k-1)-clique around curV's neighbors.
	   */
	@Deprecated
	public long[] enumCliqueOfSize(int cliqueSize, long curV) {
		encoder = new CliqueEncoder(curV, cliqueSize);
		TLongArrayList l = this.getNodeList();
		TLongArrayList neighbors = null;

		TLongIterator it = l.iterator();
		int[] indexes = new int[cliqueSize - 1];
		long[] curClique = new long[cliqueSize];
		long a = 0L;
		countRunning = 0L;

		while (it.hasNext()) {
			a = it.next();
			curClique[0] = a;
			neighbors = this.getLargerNeighbors(a);
			if (neighbors.size() >= cliqueSize - 1) {
				indexes[0] = 0;
				int fixing = 0;
				boolean failure = false;
				while (fixing >= 0) {
					while (!failure) {
						while (!failure
								&& !this.checkPreviousAreAdjacent(neighbors,
										indexes, fixing)) {
							indexes[fixing] += 1;
							if (!(indexes[fixing] < (neighbors.size() - (indexes.length - fixing - 1)))) {
								failure = true;
							}
						}
						if (!failure) {
							if (fixing + 1 < indexes.length) {							
								fixing += 1;
								indexes[fixing] = indexes[fixing - 1] + 1;
							} else {
								countRunning += 1;
								if (countRunning > 1 && countRunning % 1000000 == 1) {
									//log.info("Current clique count: " + countRunning);
								}
								for (int i = 1; i < cliqueSize; ++i) {
									curClique[i] = neighbors.get(indexes[i - 1]);
								}
								
								encoder.addNormalVertices(curClique);
								indexes[fixing] += 1;
							}
							if (!(indexes[fixing] < (neighbors.size() - (indexes.length
									- fixing - 1)))) {
								failure = true;
							}
						}
					}
					fixing -= 1;
					if (fixing >= 0) {
						indexes[fixing] += 1;
						failure = (!(indexes[fixing] < (neighbors.size() - (indexes.length
								- fixing - 1))));
					}
				}
			}
		}
		return encoder.getEncodedCliques();
	}

	
	/**
	 * New API. 
	 * Using the cliqueMap to save useless enumeration.
	 * Using the bitset to replace the hash map
     * @param cliqueSize
	 * @param curV
	 * @return
	 */
	public long[] enumClique(int cliqueSize, long curV, boolean countOnly) {
		assert(this.cliqueSet != null);
		if(cliqueSet.size() == 0) {
			if(countOnly) {
				long[] res = new long[1];
				res[0] = this.countCliquesOfSize(cliqueSize);
				return res;
			}
			else {
				return this.enumCliqueOfSize(cliqueSize, curV);
			}
		}
		
		if(countOnly) {
			encoder = null;
		}
		else {
			encoder = new CliqueEncoder(curV, cliqueSize, cliqueSet.size());
		}
		TLongArrayList l = this.getNonCliqueNodeList();

		TLongIterator it = null, it2 = null;
		TLongArrayList neighbors = new TLongArrayList();
		TLongArrayList commonNeighbors = new TLongArrayList();
		TLongArrayList curCommonNeighbors = new TLongArrayList();

		countRunning = 0L;
		it = l.iterator();
		
		long a = 0L;
			
		while (it.hasNext()) {
			a = it.next();
			neighbors = this.getNonCliqueLargerNeighbors(a);

			commonNeighbors.clear();
			this.addCommonCliqueNeighbors(a, commonNeighbors);
			
			if(commonNeighbors.size() >= cliqueSize - 1) {
				long[] single = { a };
				countRunning += CliqueEncoder.binorm(commonNeighbors.size(), 
						cliqueSize - 1);
				if(!countOnly){
					encoder.addNormalVerticesWithCompress(single, 1, 
							commonNeighbors.toArray());
				}
				//System.out.println(HyperVertex.HVArrayToString(single) + ", " +
				//		HyperVertex.HVArrayToString(commonNeighbors.toArray()));
			}
			long[] curClique = new long[2];
			curClique[0] = a;
			it2 = neighbors.iterator();
			int index = 0;
			while(it2.hasNext()) {
				long neigh = it2.next();
				curClique[1] = neigh;
				if(cliqueSize != 2) {
					curCommonNeighbors.clear();
					for (long cliqueVertex : commonNeighbors.toArray()) {
						if (this.hasNeighbor(cliqueVertex, curClique[1])) {
							curCommonNeighbors.add(cliqueVertex);
						}
					}
					if(curCommonNeighbors.size() >= cliqueSize - 2) {
						if(!countOnly) {
							encoder.addNormalVerticesWithCompress(curClique, 2, 
								curCommonNeighbors.toArray());
						}
						countRunning += CliqueEncoder.binorm(
							curCommonNeighbors.size(), cliqueSize - 2);
					}
					if (cliqueSize - 2 <= neighbors.size() - index || 
							cliqueSize - 3 <= curCommonNeighbors.size()) {
						this.enumCliqueRecursive(curClique, index,
								neighbors, curCommonNeighbors, cliqueSize);
					}
				}
				else {
					countRunning += 1;
					if(!countOnly)
						encoder.addNormalVerticesWithCompress(curClique, 2, null);
				}
				index += 1;
			}

		}
			
		if(cliqueSet.size() >= cliqueSize) {
			countRunning += CliqueEncoder.binorm(cliqueSet.size(), cliqueSize);
			if(!countOnly)
				encoder.addCliqueVertex(cliqueSet.toArray());
		}
		long[] res = null;
		if(!countOnly)
			res = encoder.getEncodedCliques();
		else {
			res = new long[1];
			res[0] = countRunning;
		}
		return res;
	}
	
	
	/**
	 * 
	 * @param curClique
	 * @param indexOfLast
	 * @param neighbors
	 * @param commonCliqueNeighbors
	 * @param countOnly
	 */
	public void enumCliqueRecursive(long[] curClique, int indexOfLast, TLongArrayList neighbors, 
			TLongArrayList commonCliqueNeighbors, int cliqueSize) {
		int curCliqueSize = curClique.length;
		long[] cliqueNodes = commonCliqueNeighbors.toArray();
		TLongHashSet curCommonCliqueNeighbors = null;
		TLongArrayList removeNodes = null;

		
		if(curCliqueSize + 1 < cliqueSize) {
			curCommonCliqueNeighbors = new TLongHashSet(cliqueNodes);
			removeNodes = new TLongArrayList();
			
			if (curCliqueSize > 2) {
				for (long cliqueNode : cliqueNodes) {
					for (int i = 2; i < curClique.length; ++i) {
						if (!this.hasNeighbor(cliqueNode, curClique[i])) {
							curCommonCliqueNeighbors.remove(cliqueNode);
						}
					}
				}
			}
		}
		
		for(int i = indexOfLast + 1; i < neighbors.size(); ++i) {
			long newNode = neighbors.get(i);
			if(this.checkFormClique(curClique, newNode)) {
				long[] newClique = new long[curCliqueSize + 1];
				for(int j = 0; j < curCliqueSize; ++j) {
					newClique[j] = curClique[j];
				}
				newClique[curCliqueSize] = newNode;
				
				if(curCliqueSize + 1 == cliqueSize) {
					countRunning += 1;
					if(encoder != null)
						encoder.addNormalVerticesWithCompress(newClique, cliqueSize, null);
				}
				else {
					removeNodes.clear();

					for(long cliqueNode: curCommonCliqueNeighbors.toArray()) {
						if (!this.hasNeighbor(cliqueNode, newNode)) {
							curCommonCliqueNeighbors.remove(cliqueNode);
							removeNodes.add(cliqueNode);
						}
					}
					countRunning += CliqueEncoder.binorm(curCommonCliqueNeighbors.size(), 
								cliqueSize - (curCliqueSize + 1));
					if(encoder != null)
						encoder.addNormalVerticesWithCompress(newClique, curCliqueSize + 1, 
							curCommonCliqueNeighbors.toArray());

					if (cliqueSize - curCliqueSize <= neighbors.size() - i || 
							cliqueSize - curCliqueSize - 2 <= curCommonCliqueNeighbors.size()) {
						this.enumCliqueRecursive(newClique, i, neighbors, 
								commonCliqueNeighbors, cliqueSize);
					}
					curCommonCliqueNeighbors.addAll(removeNodes);
				}
			}
		}
	}
	

	public int getNodeDegree(long a) {
		Integer deg = degrees.get(a);
		if(deg == null) return 0;
		return deg;
	}
	
	public boolean addEdge (long a, long b) {
		if(HyperVertex.VertexID(a) == HyperVertex.VertexID(b)){
			return false;
		}
		boolean res = this.addOrientedEdge(a,b) && this.addOrientedEdge(b, a);
		if(res){
			this.unorientedSize++;
		}
		return res;
	}


	public boolean addOrientedEdge(long source, long target) {
		boolean res = false;
		TLongHashSet adj;

		if (!graph.containsKey(source)) {
			adj = new TLongHashSet();
			graph.put(source, adj);
			degrees.put(source, 0);
		} else {
			adj = graph.get(source);
		}
		if(!adj.contains(target)){
			adj.add(target);
			this.degrees.increment(source);
			this.orientedSize++;
			res = true;
		}
		return res;
	}
	
	/**
	 * Get adjacency list of a vertex: sorted from low degree to high degree
	 * @param vertex
	 * @return
	 */
	public TLongArrayList getSortedAdjList(long vertex){
		if(!graph.containsKey(vertex)){
			return null;
		}
		TLongArrayList list = new TLongArrayList();
		list.addAll(graph.get(vertex).toArray());
		list.sort();
		return list;
	}
	
	public TLongArrayList getAdjList(long vertex){
		if(!graph.containsKey(vertex)){
			return null;
		}
		TLongArrayList list = new TLongArrayList();
		list.addAll(graph.get(vertex).toArray());
		return list;
	}

	public boolean hasNeighbor(long source, long target) {
		return (graph.containsKey(source) && graph.get(source).contains(target));
	}
	
	public boolean removeEdge (long a, long b) {

		boolean res = this.removeOrientedEdge(a,b) && this.removeOrientedEdge(b,a);
		if(res) {
			this.unorientedSize--;
		}
		return res;
	}

	public boolean removeOrientedEdge(long source, long target) {

		TLongHashSet adj;

		if (graph.containsKey(source)) {
			adj = graph.get(source);
			if(adj.contains(target)){	
				adj.remove(target);
				if(adj.isEmpty()){
					graph.remove(source);
					degrees.remove(source);
				} else {
					degrees.put(source, this.getNodeDegree(source) - 1);
				}
				this.orientedSize--;
				return true;
			}
		}
		return false;
	}

	public long getUnorientedSize() {
		return unorientedSize;
	}

	/*public void setUnorientedSize(long unorientedSize) {
		this.unorientedSize = unorientedSize;
	}*/

	public long getOrientedSize() {
		return orientedSize;
	}
	
	/**
	 * Finding cliques whose size exceed cliqueSizeThresh
	 * @param cliqueSizeThresh
	 * @return
	 */
	public Collection<HyperVertexHeap> findCliqueCover(int cliqueSizeThresh){
		ArrayList<HyperVertexHeap> alreadyFoundClique = new ArrayList<HyperVertexHeap>();
		PriorityQueue<Long> queue = new PriorityQueue<Long>(
				Config.HEAPINITSIZE, new GraphNodeComparator(this.degrees));
		for(long v : this.getNodeList().toArray()){
			queue.add(v);
		}
		
		while(!queue.isEmpty()){
			long nextV = queue.poll();
			//System.out.println("Detecting: " + HyperVertex.toString(nextV));
			int i = 0;
			for(HyperVertexHeap clique : alreadyFoundClique){
				if(checkFormClique(nextV, clique)){
					clique.insert(nextV);
					break;
				}
				++i;
			}
			if(i == alreadyFoundClique.size()){
				HyperVertexHeap newClique = new HyperVertexHeap(Config.HEAPINITSIZE);
				newClique.insert(nextV);
				alreadyFoundClique.add(newClique);
			}
		}
		for(int i = 0; i < alreadyFoundClique.size(); ++i){
			HyperVertexHeap clique = alreadyFoundClique.get(i);
			if(clique.size() >= cliqueSizeThresh){
				clique.sort();
			}
			else{
				alreadyFoundClique.remove(i);
				clique.clear();
			}
		}
		return alreadyFoundClique;
	}
	
	private boolean checkFormClique(long v, HyperVertexHeap clique){
		boolean res = true;
		for(long u : clique.toArrays()){
			if(!this.hasNeighbor(v, u)){
				res = false;
				break;
			}
		}
		return res;
	}
	
	/**
	 * Remove some edges to make all vertices' degree smaller than degThresh
	 * @param degThresh The threshold of the degree
	 * @param option Options for the choice of which edge to remove: 
	 * option = 0: Remove randomly;
	 * option = 1: Remove those edges connecting larger-degree vertices;
	 * option = 2: Remove those edges connecting smaller-degree vertices;
	 */
	public void makeDegLimitGraph(int degThresh, int option){
		if(option != 0 && option != 1 && option != 2){
			System.err.println("option = 0: Remove randomly;");
			System.err.println("option = 1: Remove those edges connecting larger-degree vertices;");
			System.err.println("option = 2: Remove those edges connecting smaller-degree vertices;");
			return;
		}
		System.out.println("Degree Threshold : " + degThresh);
		TLongIterator iter = this.getNodeList().iterator();
		while(iter.hasNext()){
			long curVertex = iter.next();
			if(this.getNodeDegree(curVertex) > degThresh){
				System.out.println("Processing large-degree vertex: id = " + 
						HyperVertex.VertexID(curVertex) + " ; deg = " + this.getNodeDegree(curVertex));
				TLongArrayList ngrs = null;
				int numEdgesToRemove = this.getNodeDegree(curVertex) - degThresh;
				switch(option){
					case 0:
						ngrs = this.getAdjList(curVertex);
						ngrs.shuffle(new Random(System.currentTimeMillis()));			
						break;
					case 1:
						ngrs = this.getSortedAdjList(curVertex);
						ngrs.reverse();
						break;
						
					case 2:
						ngrs = this.getSortedAdjList(curVertex);
						break;
					default:
						ngrs = this.getAdjList(curVertex);
						ngrs.shuffle(new Random(System.currentTimeMillis()));	
						break;
				}
				//System.out.println("current vertex: " + HyperVertex.VertexID(curVertex));
				//Utility.printArray(ngrs.toArray());
				for(long n : ngrs.toArray()){
					this.removeEdge(curVertex, n);
					if((--numEdgesToRemove) == 0){
						break;
					}
				}
				System.out.println("After Edge Removing, its degree becomes: " + this.getNodeDegree(curVertex));
				ngrs.clear();
				ngrs = null;
			}
		}
		
	}
	
	/**
	 * Dump the graph to the file
	 * @param filename
	 */
	public void dumpGraph(String filename) {
		File file = new File(filename);
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(file));
			for (long node : this.getNodeList().toArray()) {
				for (long ngr : this.getAdjList(node).toArray()) {
					if (HyperVertex.VertexID(node) < HyperVertex.VertexID(ngr)) {
						String edgeStr = HyperVertex.VertexID(node) + "\t"
								+ HyperVertex.VertexID(ngr);
						if (!file.exists()) {
							System.out.println(edgeStr);
						} else {
							writer.write(edgeStr + "\n");
						}
					}
				}
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Set local clique set
	 * @param set
	 */
	public void setLocalCliqueSet(TLongHashSet set) {
		this.cliqueSet = set;
	}
	
	/**
	 * Clear local clique set
	 */
	public void clearLocalCliqueSet() {
		this.cliqueSet.clear();
	}
	
	/**
	 * Add node to local clique set
	 * @param a
	 */
	public void addSetNodes(long a) {
		this.cliqueSet.add(a);
	}
	
	public int getLocalCliqueSetSize() {
		return this.cliqueSet.size();
	}
	
	/**
	 * As nodes in local clique set form a large clique, 
	 * in the case that the local clique set is relatively small (<20) when
	 * we do not apply the clique compression, we simply add the edges
	 * (must be mutually connected among vertices in local clique set) back
	 * and the clique enumeration will follow the normal routine.
	 */
	public void unfoldLocalCliqueSet() {
		long[] array = this.cliqueSet.toArray();
		for(int i = 0; i < array.length - 1; ++i) {
			for(int j = i + 1; j < array.length; ++j) {
				this.addEdge(array[i], array[j]);
			}
		}
		this.cliqueSet.clear();
	}
	
	/**
	 * Generate hyper graph
	 */
	public void genHyperGraph() {
		TLongHashSet alreadyVisitSet = new TLongHashSet();
		TLongIntHashMap hyperVertexMap = new TLongIntHashMap();
		TLongIterator iter = this.getNodeList().iterator();
		TLongIterator iterNeigh, iterNeigh2;
		long currentHyper = 0;
		int numHyperVertices = 0, numHyperEdges = 0;
		while(iter.hasNext()){
			long v = iter.next();
			if(!alreadyVisitSet.contains(v)){
				alreadyVisitSet.add(v);
				// Creat a hypervertex is isclique = false
				currentHyper = HyperVertex.get(HyperVertex.VertexID(v), false, 0, 0);
				//System.out.println("Gen a new hyper vertex: " + HyperVertex.VertexID(v));
				++numHyperVertices;
				hyperVertexMap.put(v, HyperVertex.VertexID(currentHyper));
				iterNeigh = this.getAdjList(v).iterator();
				while(iterNeigh.hasNext()){
					long v1 = iterNeigh.next();
					if(this.isEquivalent(v, v1)){
						currentHyper = HyperVertex.setClique(currentHyper, true);
						hyperVertexMap.put(v1, HyperVertex.VertexID(currentHyper));
						alreadyVisitSet.add(v1);
					}
				}
				if(!HyperVertex.isClique(currentHyper)){
					iterNeigh = this.getAdjList(v).iterator();
					while(iterNeigh.hasNext()){
						long v1 = iterNeigh.next();
						iterNeigh2 = this.getAdjList(v1).iterator();
						while(iterNeigh2.hasNext()){
							long v2 = iterNeigh2.next();
							if(v == v2){
								continue;
							}
							if(this.isEquivalent(v, v2)){
								hyperVertexMap.put(v2, HyperVertex.VertexID(currentHyper));
								alreadyVisitSet.add(v2);
							}
						}
					}
				}
			}
		}
		alreadyVisitSet.clear();
		iter = this.getNodeList().iterator();
		while(iter.hasNext()){
			long v = iter.next();
			iterNeigh = this.getAdjList(v).iterator();
			while(iterNeigh.hasNext()){
				long v1 = iterNeigh.next();
				if(v < v1){
					if(hyperVertexMap.get(v) != hyperVertexMap.get(v1)){
						++numHyperEdges;
					}
				}
			}
		}
		System.out.println("# of hyper vertices: " + numHyperVertices);
		System.out.println("# of hyper edges: " + numHyperEdges);
		hyperVertexMap.clear();
	}
	
	/**
	 * Verify whether two nodes are equivalent
	 * @param v1
	 * @param v2
	 * @return
	 */
	public boolean isEquivalent(long v1, long v2){
		boolean res = false;
		if(this.getNodeDegree(v1) == 0 || this.getNodeDegree(v2) == 0 ||
				this.getNodeDegree(v1) != this.getNodeDegree(v2)){
			return res;
		}
		int count = 0;
		TLongIterator iter = this.getAdjList(v2).iterator();
		while(iter.hasNext()){
			long v3 = iter.next();
			if(v3 == v1){
				++count;
			}
			else{
				if(!this.hasNeighbor(v1, v3)){
					return res;
				}
				++count;
			}
		}
		if(count == this.getNodeDegree(v1)){
			res = true;
		}
		return res;
	}
	
	public void clear() {
		if(this.cliqueSet != null)
			this.cliqueSet.clear();
		this.degrees.clear();
		if(this.encoder != null)
			this.encoder.clear();
		this.encoder = null;
		for(long key : this.graph.keySet()) {
			this.graph.get(key).clear();
		}
		this.graph.clear();
		
		unorientedSize=0;
		orientedSize=0;
		countRunning = 0;
	}
}