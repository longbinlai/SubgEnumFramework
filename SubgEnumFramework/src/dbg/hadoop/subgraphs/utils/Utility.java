package dbg.hadoop.subgraphs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.inject.Key;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.IntegerPairWritable;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Utility{
	private static String defaultFS = "";

	private static FileSystem fs;
	
	public static TIntIntHashMap deg = new TIntIntHashMap();
	
	public static HashMap<Long, int[]> hyperVertex = new HashMap<Long, int[]>();
	
	public static TLongObjectHashMap<long[]> clique = null;
	
	public static int count = 0;
	
	// The inverted list of cliqueMap in the form of vertex : cliqueID
	public static TLongLongHashMap cliqueMap = null;
	
	public static void main(String[] args){
		long[] array = {1,2,3,4,5};
		long cur = 0;
		int keyMap = 0x15;
		int starSize = 5;
		
		genStars(cur, array, starSize, 0, keyMap);
		
		System.out.println("Num Stars: " + count);
		
	}
	
	public static class VertexComparator implements Comparator<Integer>{
		@Override
		public int compare(Integer v1, Integer v2) {
			// TODO Auto-generated method stub
			return Utility.compareVertex(v1, v2);
		}
		
	}
	
	public static class HyperVertexComparator implements Comparator<Long>{
		@Override
		public int compare(Long v1, Long v2) {
			// TODO Auto-generated method stub
			return HyperVertex.compare(v1, v2);
		}
		
	}
	
	public static void setDefaultFS(String name) throws IOException{
		//System.out.println("Set defaultFS: " + name);
		defaultFS = name;
		Configuration conf = new Configuration();
		if(defaultFS.contains("hdfs://")){
			conf.set("fs.default.name", defaultFS);
		}
		fs = FileSystem.get(conf);
	}
	
	public static FileSystem getFS(){
		return fs;
	}
	
	public static void readDegreeFile(Configuration conf) throws IOException{
		//System.out.println("In readDegreeFile...");
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		for (int i = 0; i < paths.length; ++i) {
			//System.out.println("path name is: " + paths[i].toString());
			if (paths[i].toString().contains(Config.degreeFileDir)) {
				File root = new File(paths[i].toString());
				for (File f : root.listFiles()) {
					//System.out.println("File name is: " + f.toString());
					if (f.toString().contains("part-r-") & 
							!f.toString().endsWith(".crc")) {
						BufferedReader reader = new BufferedReader(
								new FileReader(f));
						String line = "";
						int key = 0;
						int value = 0;
						
						//System.out.println("File = " + f);
						while ((line = reader.readLine()) != null) {
							//System.out.println(line);
							key = Integer.parseInt(line.split("\t")[0]);
							value = Integer.parseInt(line.split("\t")[1]);
							if (!deg.containsKey(key)) {
								deg.put(key, value);
							}
						}
						reader.close();
					}
				}
			}
		}
	}
	
	public static void readHyperVertex(Configuration conf) throws 
		IOException, InstantiationException, IllegalAccessException{
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		for (int i = 0; i < paths.length; ++i) {
			if (paths[i].toString().contains(Config.hyperVertex)) {
				File root = new File(paths[i].toString());
				for (File f : root.listFiles()) {
					if (f.toString().contains("part-r-") && 
							!f.toString().endsWith(".crc")) {
						SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(conf),
								new Path(f.toString()), conf);

						LongWritable key = (LongWritable) reader.getKeyClass().newInstance();
						IntegerPairWritable value = (IntegerPairWritable) reader
								.getValueClass().newInstance();
						while (reader.next(key, value)) {
							if(!hyperVertex.containsKey(key.get()) && HyperVertex.Size(key.get()) > 1){
								hyperVertex.put(key.get(), value.getIntegers());
							}
						}
						reader.close();
					}
				}
			}
		}
	}
	
	public static void readCliques(Configuration conf, boolean invertedList) throws 
		IOException, InstantiationException, IllegalAccessException{
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		clique = new TLongObjectHashMap<long[]>();
		cliqueMap = new TLongLongHashMap();
		
		for (int i = 0; i < paths.length; ++i) {
			if (paths[i].toString().contains(Config.cliques)) {
				File root = new File(paths[i].toString());
				for (File f : root.listFiles()) {
					if (f.toString().contains("part-r-") && 
							!f.toString().endsWith(".crc")) {
						SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(conf),
								new Path(f.toString()), conf);

						LongWritable key = (LongWritable) reader.getKeyClass().newInstance();
						HVArray value = (HVArray) reader
								.getValueClass().newInstance();
						while (reader.next(key, value)) {
							if(!invertedList){
								if(!clique.contains(key.get())){
									clique.put(key.get(), value.toArrays());
								}
							}
							else{
								for(long v : value.toArrays()){
									if(!cliqueMap.contains(v)){
										cliqueMap.put(v, key.get());
									}
								}
							}
						}
						reader.close();
					}
				}
			}
		}
	}
	
	public static void readHyperVertexLocally(String dir) throws IOException,
			InstantiationException, IllegalAccessException {
		FileStatus[] status = fs.listStatus(new Path(dir));
		// System.out.println(degreeFileDir);
		
		for (int i = 0; i < status.length; ++i) {
			String p = status[i].getPath().toString();
			if (p.contains("part-r-") & !p.contains(".crc")) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs,
						new Path(p), new Configuration());
				LongWritable key = (LongWritable) reader.getKeyClass()
						.newInstance();
				IntegerPairWritable value = (IntegerPairWritable) reader
						.getValueClass().newInstance();
				while (reader.next(key, value)) {
					if (!hyperVertex.containsKey(key.get())) {
						hyperVertex.put(key.get(), value.getIntegers());
					}
				}
				reader.close();
			}
		}
		
		BufferedWriter writer = new BufferedWriter(
				new FileWriter(new File("/home/robeen/scripts/hypergraph/hyperVertex.out")));
		
		for(long hv: hyperVertex.keySet()){
			String res = HyperVertex.toString(hv) + "\t" + "{";
			for(int v : hyperVertex.get(hv)){
				res += v + ",";
			}
			res = res.substring(0, res.length() - 1);
			writer.write(res + "}\n");
		}
		writer.close();
	}
	
	/**
	 * @param vertex1
	 * @param vertex2
	 * @return 
	 *  0 : vertex1 = vertex2 <br>
	 *  1 : deg(vertex1) > deg(vertex2) or when deg(vertex1) = deg(vertex2), vertex1 > vertex2 <br>
	 *  -1 : deg(vertex1) < deg(vertex2) or when deg(vertex1) = deg(vertex2), vertex1 < vertex2  
	 */
	public static int compareVertex(int vertex1, int vertex2){
		int res = 0;
		if(vertex1 == vertex2){
			return res;
		}
		res = (vertex1 < vertex2) ? -1 : 1;
		if(deg.containsKey(vertex1) && deg.containsKey(vertex2)){
			if(deg.get(vertex1) > deg.get(vertex2)){
				res = 1;
			}
			else if(deg.get(vertex1) < deg.get(vertex2)){
				res = -1;
			}
		}
		return res;	
	}
	
	/**
	 * Intersect two arrays
	 * @param array1: First Array
	 * @param array2: Second Array
	 * 
	 * @return The intersection of the two input arrays
	 */
	public static int[] intersection(int[] array1, int[] array2){
		int minSize = Math.min(array1.length, array2.length);
		int[] res = new int[minSize];
		int pos = 0, k = 0;
		for(int i = 0; i < array1.length; ++i){
			if(pos == array2.length){
				break;
			}
			for(int j = pos; j < array2.length; ++j){
				if(compareVertex(array1[i], array2[j]) < 0){
					pos = j;
					break;
				}
				else if(compareVertex(array1[i], array2[j]) == 0){
					res[k++] = array1[i];
					pos = j + 1;
				}
				else{
					continue;
				}
			}
		}
		return Arrays.copyOf(res, k);
	}
	
	/**
	 * Intersect two arrays
	 * @param array1: First Array
	 * @param array2: Second Array
	 * 
	 * @return The intersection of the two input arrays
	 */
	public static long[] intersection(long[] array1, long[] array2){
		int minSize = Math.min(array1.length, array2.length);
		long[] res = new long[minSize];
		if(minSize == 0){
			return res;
		}
		int pos = 0, k = 0;
		for(int i = 0; i < array1.length; ++i){
			if(pos == array2.length){
				break;
			}
			for(int j = pos; j < array2.length; ++j){
				if(HyperVertex.compare(array1[i], array2[j]) < 0){
					pos = j;
					break;
				}
				else if(HyperVertex.compare(array1[i], array2[j]) == 0){
					res[k++] = array1[i];
					pos = j + 1;
					break;
				}
				else{
					continue;
				}
			}
		}
		return Arrays.copyOf(res, k);
	}
	
	public static boolean isValidCand(long[] array){
		if(array == null || array.length == 0){
			return false;
		}
		TLongIntHashMap map = new TLongIntHashMap();
		for(long hypervertex: array){
			if(map.containsKey(hypervertex)){
				//map.put(hypervertex, map.get(hypervertex) + 1);
				map.increment(hypervertex);
			}
			else{
				map.put(hypervertex, 1);
			}
		}
		for(long hypervertex: map.keys()){
			if(HyperVertex.Size(hypervertex) < map.get(hypervertex)){
				return false;
			}
		}
		return true;
	}
	
	
	public static void printMemoryUsage(){
		StringBuilder sb = new StringBuilder();
		Runtime runtime = Runtime.getRuntime();
		NumberFormat format = NumberFormat.getInstance();
		
		long maxMemory = runtime.maxMemory();
		long allocateMemory = runtime.totalMemory();
		long freeMemory = runtime.freeMemory();
		
		sb.append("free memory: " + format.format(freeMemory / (1024.0 * 1024.0)) + "MB.\n");
		sb.append("used memory: " + format.format((allocateMemory - freeMemory) / (1024.0 * 1024.0)) + "MB.");
		System.out.println(sb);
	}
	
	public static void printMap(Map<Integer, ArrayList<Integer>> map){
		if(map == null || map.isEmpty()){
			return;
		}
		
		for(int key: map.keySet()){
			System.out.print("map key = " + key + ": {");
			for(int value: map.get(key)){
				System.out.print(value + "|");
			}
			System.out.println("}");
		}
		
	}
	
	public static void printArray(int[] array){
		if(array == null || array.length == 0){
			return;
		}
		System.out.print("{");
		for(int v : array){
			System.out.print(v + ",");
		}
		System.out.println("}");
	}
	
	public static void printArray(long[] array){
		if(array == null || array.length == 0){
			return;
		}
		System.out.print("{");
		for(long v : array){
			System.out.print(HyperVertex.toString(v) + ",");
		}
		System.out.println("}");
	}
	
	/**
	 * Get the working dir for current file. 
	 * Normally, it will include the slash. 
	 * For example, if the @(inputFile) is workDir/input.txt, we will return workDir. 
	 * @param inputFile
	 * @return
	 */
	public static String getWorkDir(String inputFile){
		int lastSlash = inputFile.lastIndexOf("/");
		String dir = "";
		if(lastSlash != 0){
			dir = inputFile.substring(0, lastSlash + 1);
		}
		else{
			dir = "";
		}
		return dir;
	}
	
	public static String getFileName(String inputFile){
		int lastSlash = inputFile.lastIndexOf("/");
		String filaname = "";
		if(lastSlash != 0){
			filaname = inputFile.substring(lastSlash + 1);
		}
		else{
			filaname = "";
		}
		return filaname;
	}
	
	public static String getNewDir(String inputFile){
		int lastSlash = inputFile.lastIndexOf("/");
		String dir = "";
		if(lastSlash != 0){
			dir = inputFile.substring(0, lastSlash) + ".new" + "/";
		}
		else{
			dir = "0.new";
		}
		
		return dir;
	}
	
	public static ArrayList<long[]> partArray(long[] array, int thresh){
		ArrayList<long[]> arrayPartitioner = new ArrayList<long []>();
		if(thresh == 0 || thresh > array.length){
			arrayPartitioner.add(array);
			return arrayPartitioner;
		}
		int numGroups = array.length / thresh;
		if (array.length - numGroups * thresh > thresh * 0.1) {
			if (numGroups != 0) {
				numGroups += 1;
			}
		}
		int from = 0, to = 0;
		for (int i = 0; i < numGroups; ++i) {
			from = i * thresh;
			if (i == numGroups - 1) {
				to = array.length;
			} else {
				to = (i + 1) * thresh;
			}
			arrayPartitioner.add(Arrays.copyOfRange(array, from, to));
		}
		return arrayPartitioner;
	}
	
	/**
	 * Sampling the vertex
	 * @param sampleRate sample rate
	 * @param samplePath the path to install the sample vertices
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void sampleVertex(float sampleRate, String sampleDir, String workDir) 
			throws IOException, InstantiationException, IllegalAccessException{
		Path degreePath = new Path(workDir + Config.degreeFileDir);
		Path samplePath = new Path(workDir + sampleDir);
		if(!fs.exists(degreePath)) {
			System.err.println("No degree file exist...");
			return;
		}
		int sampleValue = (int)(100 * sampleRate);
		Random rand = new Random(System.currentTimeMillis());
		FileStatus[] status = fs.listStatus(degreePath);
		if (fs.exists(samplePath)) {
			fs.delete(samplePath, true);
		}
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				fs.create(samplePath)));
		
		IntWritable key = null;
		IntWritable val = null;
		for(int i = 0; i < status.length; ++i){
			String p = status[i].getPath().toString();
			if (p.contains("part-") & !p.contains(".crc")) {

				SequenceFile.Reader reader = new SequenceFile.Reader(fs, status[i].getPath(), 
						new Configuration());
				key = (IntWritable) reader.getKeyClass()
						.newInstance();
				val = (IntWritable) reader.getValueClass()
						.newInstance();
				
				while (reader.next(key, val)) {
					if(rand.nextInt(100) < sampleValue){
						writer.write(key.get() + "\n");
					}
				}
				reader.close();
			}
		}
		writer.close();
	}
	
	/**
	 * Generate key-value pairs using result and keyMap.
	 * For example, if {@code result} = {1,2,3,4,5,6,7}, and keyMap = 0110101,
	 * then the key should be {2,3,5,7} and value is {1,4,6}
	 * @param result
	 * @param keyMap
	 * @return
	 */
	public static long[][] getKeyValuePair(long[] result, int keyMap){
		assert(keyMap < (1 << result.length));
		long[][] kvPair = new long[2][];
		int size = result.length;
		int keyBits = Integer.bitCount(keyMap);
		int valBits = size - keyBits;
		long[] key = new long[keyBits];
		long[] value = new long[valBits];
		
		int i = 0, j = 0;
		
		int count = size - 1;
		while(count >= 0){
			if((keyMap & (1 << count)) == 0){
				value[j++] = result[size - count - 1];
			}
			else{
				key[i++] = result[size - count - 1];
			}
			--count;
		}
		kvPair[0] = key;
		kvPair[1] = value;
		return kvPair;
	}
	
	
	public static void genStars(long cur, long[] array, int starSize, int sign, int keyMap){
		TLongArrayList result = new TLongArrayList(starSize);
		result.add(cur);
		genStarsRecur(cur, array, starSize, sign, keyMap, 1, 0, result);
	}
	
	public static void genStarsRecur(long cur, long[] array, int starSize, int sign, int keyMap, 
			int curLevel, int curIndex, TLongArrayList result){	
		if(curLevel == starSize){
			++count;
			long[][] kv = Utility.getKeyValuePair(result.toArray(), keyMap);
			//context.write(new HVArraySign(new HVArray(kv[0]), sign), new HVArray(kv[1])); 
			System.out.println("key: ");
			for(int i = 0; i < kv[0].length; ++i){
				System.out.print(kv[0][i] + ",");
			}
			System.out.println("\nvalue: ");
			for(int i = 0; i < kv[1].length; ++i){
				System.out.print(kv[1][i] + ",");
			}
			System.out.println("\n");
		}
		else{
			long tmp = 0L;
			for(int i = curIndex; i < array.length; ++i){
				tmp = array[i];
				//if(this.isFeasible(cur, result, curLevel)){
				if(result.size() <= curLevel) result.add(tmp);
				else result.set(curLevel, tmp);
				genStarsRecur(cur, array, starSize, sign, keyMap, curLevel + 1, i + 1, result);
				//}
			}
		}
		
	}
	
	
}