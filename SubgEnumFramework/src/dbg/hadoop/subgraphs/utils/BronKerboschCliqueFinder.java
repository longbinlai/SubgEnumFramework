package dbg.hadoop.subgraphs.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

/**
 * Adopted from JGraphT for use with Jung.
 * The Brons Kerbosch algorithm is an algorithm for finding maximal cliques in an undirected graph
 * This algorithmn is taken from Coenraad BronJoep Kerbosch in 1973.
 * This works on undirected graph
 *  See {@linktourl  See http://en.wikipedia.org/wiki/Bron%E2%80%93Kerbosch_algorithm}
 * @author Reuben Doetsch
 *
 * @param <long> vertex class of graph
 * @param <E> edge class of graph
 */
public class BronKerboschCliqueFinder
{
    //~ Instance fields --------------------------------------------------------

    private final Graph graph;

    private ArrayList<TLongHashSet> cliques;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new clique finder. Make sure this is a simple graph.
     *
     * @param graph the graph in which cliques are to be found; graph must be
     * simple
     */
    public BronKerboschCliqueFinder(Graph graph)
    {

        this.graph = graph;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Finds all maximal cliques of the graph. A clique is maximal if it is
     * impossible to enlarge it by adding another vertex from the graph. Note
     * that a maximal clique is not necessarily the biggest clique in the graph.
     *
     * @return Collection of cliques (each of which is represented as a Set of
     * vertices)
     */
    public Collection<TLongHashSet> getAllMaximalCliques()
    {
        // TODO:  assert that graph is simple

        cliques = new ArrayList<TLongHashSet>();
        TLongArrayList potential_clique = new TLongArrayList();
        TLongArrayList candidates = new TLongArrayList();
        TLongArrayList already_found = new TLongArrayList();
        candidates.addAll(graph.getNodeList());
        findCliques(potential_clique, candidates, already_found);
        return cliques;
    }
    
    public Collection<long[]> getNonOverlapMaximalClique(int cliqueSizeThresh){
    	List<long[]> res = new ArrayList<long[]>();
    	cliques = new ArrayList<TLongHashSet>();
        TLongArrayList potential_clique = new TLongArrayList();
        TLongArrayList candidates = new TLongArrayList();
        TLongArrayList already_found = new TLongArrayList();
        for(long v : graph.getNodeList().toArray()){
        	if(graph.getNodeDegree(v) >= cliqueSizeThresh){
        		candidates.add(v);
        	}
        }
        findCliques(potential_clique, candidates, already_found);
        // Simply borrow hypervertex heap
        // I want to know the largest maximalclique
        // currently the hypervertex = index + cliqueSize
        // In the heap it will be reordered according to the cliqueSize
        HyperVertexHeap heap = new HyperVertexHeap(Config.HEAPINITSIZE);
        for(int i = 0 ; i < cliques.size(); ++i){
        	heap.insert(HyperVertex.get(i, cliques.get(i).size()));
        }
        heap.sort();
        // Here I want to return non-overlapped maximal clique, so
        // I have to put those vertices already assigned to specific clique into
        // the alreadyAssignedVertices and get rid of it in any output clique later;
        TLongHashSet alreadyAssignedVertices = new TLongHashSet();
        // Reversely read the heap from large to small
        long heapArray[] = heap.toArrays();
        //HyperVertexHeap sortedSet = null;
        for(int i = heapArray.length - 1; i >= 0 ; --i){
        	int index = HyperVertex.VertexID(heapArray[i]);
        	TLongHashSet curClique = cliques.get(index);
        	if(i == heapArray.length - 1){
        		alreadyAssignedVertices.addAll(curClique);
        		//res.add(curClique);
        		//sortedSet = new HyperVertexHeap(curClique.toArray());
        		//sortedSet.sort();
        		res.add(curClique.toArray());
        		//curClique.clear();
        	}
        	else{
        		for(long v: curClique.toArray()){
        			if(!alreadyAssignedVertices.contains(v)){
        				alreadyAssignedVertices.add(v);
        			}
        			else{
        				curClique.remove(v);
        			}
        		}
        		if(curClique.size() >= cliqueSizeThresh){
        			//sortedSet = new HyperVertexHeap(curClique.toArray());
            		//sortedSet.sort();
            		res.add(curClique.toArray());
            		//curClique.clear();
        		}
        	}
        }
        return res;
    }

    /**
     * Finds the biggest maximal cliques of the graph.
     *
     * @return Collection of cliques (each of which is represented as a Set of
     * vertices)
     */
    public Collection<TLongHashSet> getBiggestMaximalCliques()
    {
        // first, find all cliques
        getAllMaximalCliques();

        int maximum = 0;
        Collection<TLongHashSet> biggest_cliques = new ArrayList<TLongHashSet>();
        for (TLongHashSet clique : cliques) {
            if (maximum < clique.size()) {
                maximum = clique.size();
            }
        }
        for (TLongHashSet clique : cliques) {
            if (maximum == clique.size()) {
                biggest_cliques.add(clique);
            }
        }
        return biggest_cliques;
    }
    
    public TLongArrayList getFirstLargestMaximalClique(){
    	getAllMaximalCliques();
    	 int maximum = 0;
    	 TLongHashSet target = null;
         for (TLongHashSet clique : cliques) {
             if (maximum < clique.size()) {
                 maximum = clique.size();
                 target = clique;
             }
         }
         return new TLongArrayList(target);
    }

    private void findCliques(
        TLongArrayList potential_clique,
        TLongArrayList candidates,
        TLongArrayList already_found)
    {
        TLongArrayList candidates_array = new TLongArrayList(candidates);
        if (!end(candidates, already_found)) {
            // for each candidate_node in candidates do
        	//int count = 0;
            for (long candidate : candidates_array.toArray()) {
                TLongArrayList new_candidates = new TLongArrayList();
                TLongArrayList new_already_found = new TLongArrayList();

                // move candidate node to potential_clique
                potential_clique.add(candidate);
                candidates.remove(candidate);

                // create new_candidates by removing nodes in candidates not
                // connected to candidate node
                for (long new_candidate : candidates.toArray()) {
                    if (graph.hasNeighbor(candidate, new_candidate))
                    {
                        new_candidates.add(new_candidate);
                    } // of if
                } // of for

                // create new_already_found by removing nodes in already_found
                // not connected to candidate node
                for (long new_found : already_found.toArray()) {
                    if (graph.hasNeighbor(candidate, new_found)) {
                        new_already_found.add(new_found);
                    } // of if
                } // of for

                // if new_candidates and new_already_found are empty
                if (new_candidates.isEmpty() && new_already_found.isEmpty()) {
                    // potential_clique is maximal_clique
                    cliques.add(new TLongHashSet(potential_clique));
                } // of if
                else {
                    // recursive call
                    findCliques(
                        potential_clique,
                        new_candidates,
                        new_already_found);
                } // of else

                // move candidate_node from potential_clique to already_found;
                already_found.add(candidate);
                potential_clique.removeAt(potential_clique.size() - 1);
            } // of for
        } // of if
    }

    private boolean end(TLongArrayList candidates, TLongArrayList already_found)
    {
        // if a node in already_found is connected to all nodes in candidates
        boolean end = false;
        int edgecounter;
        for (long found : already_found.toArray()) {
            edgecounter = 0;
            for (long candidate : candidates.toArray()) {
                if (graph.hasNeighbor(found, candidate)) {
                    edgecounter++;
                } // of if
            } // of for
            if (edgecounter == candidates.size()) {
                end = true;
            }
        } // of for
        return end;
    }
}


