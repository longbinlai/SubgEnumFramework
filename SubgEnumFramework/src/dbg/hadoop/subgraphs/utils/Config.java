package dbg.hadoop.subgraphs.utils;

/**
 * Set several graph related parameters
 * @author robeen
 *
 */
public class Config{
	public static final boolean undirected = true;
	//private static String bloomFilterFile = "";
	// The home dir of the job. 
	// Generally this is the .. directory of the input file
	public static final String homeDir = ".";
	// The degree file directory
	public static final String degreeFileDir = "degreeFile";
	// The bloom filter file directory
	public static final String bloomFilterFileDir = "bloomFilter2";
	public static final String undirectGraphDir = "undirected";

	public static final String preparedFileDir = "prepared2";
	public static final String adjListDir = "adjList2";
	public static final String distinctTwinTwigDir = "distinct.twinTwig";
	
	public static final String coloredVertexDir = "vertex.color";
	public static final String coloredEdgeDir = "edge.color";
	public static final String coloredAdjList = "adjList.color";
	public static final String coloredPartitionDir = "partition.color";
	
	public static final String hyperVertex = "hyper.vertex";
	public static final String hyperEdge = "hyper.edge";
	public static final String cliques = "hyper.cliques";
	
	public static final String hyperGraphAdjList = "hyper.adjList";
	public static final String hyperGraphBloomFilterDir = "hyper.bloomfilter"; 
	
	public static final double overSizeRate = 0.1;
	
	public static final int NUMLONGBITS = Long.SIZE / Byte.SIZE;
	public static final int NUMINTBITS = Integer.SIZE / Byte.SIZE;
	
	public static final int SMALLSIGN = -2;
	public static final int LARGESIGN = -1;
	
	
	public static int TRIANGLE = 0;
	public static int SQUARE = 1;
	public static int FOURCLIQUE = 2;
	public static int FIVECLIQUE = 3;
	public static int ESQUARE = 4;
	
	public static int EDGE = 0;
	public static int TWINTWIG1 = 1; // X - W - Y (W < X < Y)
	public static int TWINTWIG2 = 2; // X - W - Y (X < W < Y)
	public static int TWINTWIG3 = 3; // X - W - Y (X < Y < W)
	public static int BF_TRIANGLE = 4;
	
	public static int HEAPINITSIZE = 20; 
	
}