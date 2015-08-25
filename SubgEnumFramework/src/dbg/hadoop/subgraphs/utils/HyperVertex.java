package dbg.hadoop.subgraphs.utils;

/**
 * HyperVertex class. <br>
 * vertexId: Encode information of HyperVertexID <br>
 * degree: Current HyperVertex Degree. It is indeed the degree of any vertex inside the hypervertex. <br>
 * isClique: IS current HyperVertex is clique <br>
 * size: The number of vertices contained in current HyperVertex <br>
 * 
 * We use a long integer(64 bits) to encode all the above information as follows: <br>
 * 
 * 0 ~ 25: totally 26 bits, encode hypervertex id <br>
 * 26 ~ 46: totally 21 bits, encode degree of current hypervertex <br>
 * 47: 1 bit, encode isClique <br>
 * 48 ~ 63: 16 bits, encode size of current hypervertex 
 * 
 * @author robeen
 *
 */
public class HyperVertex{

	private final static int NUM_ID_BITS = 26;
	private final static int ID_MASK = 0x3FFFFFF;
	
	private final static int NUM_DEGREE_BITS = 21;
	private final static int DEGREE_MASK = 0x1FFFFF;
	
	private final static int NUM_BOOLEAN_BITS = 1;
	private final static int BOOLEAN_MASK = 1;
	
	private final static int NUM_SIZE_BITS = 16;
	private final static int SIZE_MASK = 0xFFFF;
	
	public final static long COMP_MASK = 0x7FFFFFFFFFFFL;
	
	/**
	 * Return a long integer to represent a hypervertex object. <br>
	 * @param vertexId HyperVertex ID
	 * @param size How many vertices are included in current HyperVertex
	 * @param isClique Is current HyperVertex a clique
	 * @param degree The degree of current HyperVertex
	 */	
	public static long get(int _vertexId, boolean _isClique,
			int _size, int _degree){
		long vertexDetail = 0;
		if (_vertexId >= (1 << NUM_ID_BITS)) {
			System.err.println("vertexId = " + _vertexId + " is too large.");
		}
		if(_size >= (1 << NUM_SIZE_BITS)){
			System.err.println("size = " + _size + " is too large.");
		}
		if(_size >= (1 << NUM_DEGREE_BITS)){
			System.err.println("degree = " + _degree + " is too large.");
		}
		vertexDetail = (long)_vertexId;
		vertexDetail |= (((long)_degree) << NUM_ID_BITS);
		if(_isClique){
			vertexDetail |= (1L << NUM_DEGREE_BITS + NUM_ID_BITS);
		}
		vertexDetail |= (((long)_size) << NUM_DEGREE_BITS + NUM_ID_BITS + NUM_BOOLEAN_BITS);
		return vertexDetail;
	}
	
	public static long get(long vertex, int _size, boolean _isClique){
		return HyperVertex.get(HyperVertex.VertexID(vertex), _isClique, _size, HyperVertex.Degree(vertex));
	}
	
	/**
	 * A HyperVertex encapsulation for normal vertex with id + degree
	 * @param _vertexId
	 * @param _degree
	 * @return
	 */
	public static long get(int _vertexId, int _degree){
		long vertexDetail = (long)_vertexId;
		vertexDetail |= (((long)_degree) << NUM_ID_BITS);
		vertexDetail |= (((long)1) << NUM_DEGREE_BITS + NUM_ID_BITS + NUM_BOOLEAN_BITS);
		return vertexDetail;
	}
	
	/**
	 * A HyperVertex encapsulation for normal vertex with id + degree
	 * @param _vertexId
	 * @param _degree
	 * @param _color
	 * @return
	 */
	public static long get(int _vertexId, int _degree, int _color){
		long vertexDetail = (long)_vertexId;
		vertexDetail |= (((long)_degree) << NUM_ID_BITS);
		vertexDetail |= (((long)_color) << NUM_DEGREE_BITS + NUM_ID_BITS + NUM_BOOLEAN_BITS);
		return vertexDetail;
	}

	
	public static int Degree(long vertexDetail){
		return (int) ((vertexDetail >> NUM_ID_BITS) & DEGREE_MASK);
	}

	public static int VertexID(long vertexDetail) {
		return (int) (vertexDetail & ID_MASK);
	}

	public static boolean isClique(long vertexDetail) {
		return ((int) ((vertexDetail >> (NUM_ID_BITS + NUM_DEGREE_BITS)) & BOOLEAN_MASK) == 1);
	}

	public static int Size(long vertexDetail) {
		return (int) ((vertexDetail >> (NUM_ID_BITS + NUM_DEGREE_BITS + 1)) & SIZE_MASK);
	}
	
	public static int Color(long vertexDetail) {
		return Size(vertexDetail);
	}

	public static int compare(long first, long second) {
		int cmp = 0;
		long cpr1 = first & COMP_MASK;
		long cpr2 = second & COMP_MASK;
		cmp = (cpr1 == cpr2) ? 0 : (cpr1 < cpr2 ? -1 : 1);
		return cmp;
	}

	
	public static String toString(long vertexDetail){
		return "[" + VertexID(vertexDetail) + "," + Degree(vertexDetail) + "]";
		
	}
	
	public static String HVArrayToString(long[] array){
		String res = "{";
		for(long v : array){
			res += HyperVertex.toString(v) + ",";
		}
		res = res.substring(0, res.length() - 1) + "}";
		return res;
	}
}