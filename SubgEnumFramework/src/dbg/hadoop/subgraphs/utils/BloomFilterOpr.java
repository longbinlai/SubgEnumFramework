package dbg.hadoop.subgraphs.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.bloomfilter.utils.BloomFilter;
import dbg.hadoop.subgraphs.utils.bloomfilter.utils.FilterFactory;


public class BloomFilterOpr{
	private String filePath;
	private String bloomFilterDir;
	private float falsePositive;
	private long elementSize;
	private int bfType;
	//private static double bloomFilterFPRate = 0.001;
	private BloomFilter bloomFilter;
	
	public static Logger log = Logger.getLogger(BloomFilterOpr.class);
	
	public FileSystem fs;
	
	public BloomFilterOpr(float _fp){
		this.falsePositive = _fp;
		this.bfType = Config.EDGE;
	}
	
	public BloomFilterOpr(float _fp, int _type){
		this.falsePositive = _fp;
		this.bfType = _type;
	}
	
	/**
	 * Create bloomFilter
	 * @param _graphDir
	 * @param _bloomFilterDir
	 * @param _fp
	 * @param _size
	 * @throws IOException
	 */
	public BloomFilterOpr(String _fileDir, String _bloomFilterDir,
			float _fp, long _size, String _dir) throws IOException{
		this.filePath = _fileDir;
		this.bloomFilterDir = _bloomFilterDir;
		this.falsePositive = _fp;
		this.elementSize = _size;
		this.bloomFilter = null;
		this.bfType = Config.EDGE;
		this.setDefaultFS(_dir);
	}
	
	public BloomFilterOpr(String _fileDir, String _bloomFilterDir,
			float _fp, long _size, int _type, String _dir) throws IOException{
		this.filePath = _fileDir;
		this.bloomFilterDir = _bloomFilterDir;
		this.falsePositive = _fp;
		this.elementSize = _size;
		this.bloomFilter = null;
		this.bfType = _type;
		this.setDefaultFS(_dir);
	}
	
	public void setGraphFilePath(String path){
		filePath = path;
	}
	
	public void setBloomFilterDir(String dir) {
		bloomFilterDir = dir;
		
	}
	
	/**
	 * This is for the api consistence
	 * @param dir Input file dir
	 * @throws IOException
	 */
	public void setDefaultFS(String dir) throws IOException{
		//System.out.println("Set defaultFS: " + name);
		Configuration conf = new Configuration();
		if (dir.toLowerCase().contains("hdfs")) {
			int pos = dir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			conf.set("fs.default.name", dir.substring(0, pos));
		} 
		fs = FileSystem.get(conf);
	}

	/**
	 * Read the HyperGraph from HDFS and use the HyperEdge information to
	 * create a bloomFilter
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void createBloomFilter() 
			throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		if(filePath.isEmpty()){
			System.err.println("No graph file defined");
			return;
		}
		bloomFilter = (BloomFilter) FilterFactory.getFilter(this.elementSize, 
				this.falsePositive, true);
		
		FileStatus[] status = fs.listStatus(new Path(filePath));
		for (int i = 0; i < status.length; ++i) {
			String p = status[i].getPath().toString();
			if (p.contains("part-") & !p.contains(".crc")) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs,
						status[i].getPath(), new Configuration());
				
				if (this.bfType == Config.EDGE) {
					LongWritable key = (LongWritable) reader.getKeyClass()
							.newInstance();
					LongWritable value = (LongWritable) reader.getValueClass()
							.newInstance();
					while (reader.next(key, value)) {
						bloomFilter.add(HyperVertex.VertexID(key.get()),
								HyperVertex.VertexID(value.get()));
					}
				}
				else if(this.bfType == Config.TWINTWIG1){
					HVArray key = (HVArray) reader.getKeyClass().newInstance();
					IntWritable value = (IntWritable) reader
							.getValueClass().newInstance();
					while (reader.next(key, value)) {
						bloomFilter.add(HyperVertex.VertexID(key.getFirst()),
								HyperVertex.VertexID(key.getSecond()));
					}
				}
				else {
					log.error("The type of bloomfilter is mis-set.");
					return;
				}
				reader.close();
			}
		}
	}
	
	
	@SuppressWarnings("deprecation")
	public void writeBloomFilter()
			throws IOException {
		String outputFile = bloomFilterDir + "/" + "bloomFilter."
				+ this.bfType + "." + this.falsePositive;
		if (fs.exists(new Path(outputFile))) {
			fs.delete(new Path(outputFile), true);
		}

		DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(
				new FSDataOutputStream(fs.create(new Path(outputFile)))));
		FilterFactory.serialize(bloomFilter, outputStream);
		outputStream.close();
	}
	
	/**
	 * 
	 * @param conf: Hadoop Configuration 
	 * @param type: TwinTwigType: EDGE, TWINTWIG1, TWINTWIG2, TWINTWIG3
	 * @param fpRate: false positive rate
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public void obtainBloomFilter(Configuration conf) throws IOException, ClassNotFoundException{
		//String bloomFilterFile = bloomFilterDir + "/" + "bloomFilter.out." + bloomFilterFPRate;
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		//BloomFilter bf = null;
		this.bloomFilter = null;
		for (int i = 0; i < paths.length; ++i) {
			if (paths[i].toString().contains("bloomFilter." + this.bfType + "." + this.falsePositive)) {
				//if(!paths[i].toString().endsWith(Config.EDGE + "." + this.falsePositive)){
				//	continue;
				//}
				DataInputStream inputStream = new DataInputStream
						(new BufferedInputStream(
								new FileInputStream(new File(paths[i].toString()))));
				this.bloomFilter = (BloomFilter) FilterFactory.deserialize(inputStream, true);
				inputStream.close();
				break;
			}

		}
	}
	
	public BloomFilter get(){
		return this.bloomFilter;
	}
	
	
}