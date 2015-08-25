package dbg.hadoop.subgraphs.utils.bloomfilter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import dbg.hadoop.subgraphs.utils.bloomfilter.utils.BloomFilter;
import dbg.hadoop.subgraphs.utils.bloomfilter.utils.FilterFactory;

public class BloomFilterTest{
	public static void main(String []args) throws IOException{
		BloomFilter bf = null;
		
		int ELEMENTS = 20000;
		long size = ELEMENTS * (ELEMENTS - 1) / 2;
		bf = (BloomFilter) FilterFactory.getFilter(size, 0.001, true);

		//for(int i = 0; i < ELEMENTS - 1; ++i){
		//	for(int j = i + 1; j < ELEMENTS; ++j){
		//		bf.add(i + "," + j);
		//	}
		//}
		
		//for(int i = 0; i < ELEMENTS - 1; ++i){
		//	for(int j = i + 1; j < ELEMENTS; ++j){
		//		if(bf.test(i + "," + j));
		//	}
		//}
		
		System.out.println("Load data...");
		long begin = System.currentTimeMillis();
		for(int i = 0; i < ELEMENTS - 1; ++i){
			for(int j = i + 1; j < ELEMENTS; ++j){
				bf.add(i, j);
				//bf.add(i + "," + j);
			}
		}
		long end = System.currentTimeMillis();
		
		System.out.println("loading time = " + (float)(end - begin) / 1000);
		
		int fp = 0, tp = 0;
		System.out.println("Check running time");
		begin = System.currentTimeMillis();
		for(int i = 0; i < ELEMENTS - 1; ++i){
			for(int j = i + 1; j < ELEMENTS; ++j){
				if(bf.test(i, j)){
					++tp;
				}
				//bf.test(i + "," + j);
			}
		}
		
		for(int i = ELEMENTS; i < 2 * ELEMENTS - 1; ++i){
			for(int j = i + 1; j < 2 * ELEMENTS; ++j){
				if(bf.test(i, j)){
					++fp;
				}
				//bf.test(i + "," + j);
			}
		}
		
		System.out.println("TP = " + (float) tp / size + "; FP = " + (float) fp / size);
		end = System.currentTimeMillis();
		System.out.println("check time = " + (float)(end - begin) / 1000);
		
		/*
		long ELEMENTS = 1806067135;
		long begin = System.currentTimeMillis();
		bf = (BloomFilter) FilterFactory.getFilter(ELEMENTS, 0.001, true);
		
		for(long i = 0; i < ELEMENTS; ++i){
			bf.add(String.valueOf(i));
		}
		
		int falsePos = 0;
		for(long i = ELEMENTS; i < 2 * ELEMENTS; ++i){
			if(bf.test(String.valueOf(i))){
				++falsePos;
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("time = " + (end - begin));
		System.out.println("falsePos = " + ((double)falsePos) / ELEMENTS);

		File outputFile = new File("/home/robeen/dataset/bloomfilter.bin");
		if(!outputFile.exists()){
			outputFile.createNewFile();
		}
		
		DataOutputStream dos = 
				new DataOutputStream(new BufferedOutputStream
						(new FileOutputStream(outputFile)));
		
		FilterFactory.serialize(bf, dos);
		dos.close();
		bf.clear();
		

		DataInputStream dis = 
				new DataInputStream(new BufferedInputStream
						(new FileInputStream(outputFile)));
		
		BloomFilter newbf = (BloomFilter) FilterFactory.deserialize(dis, true);
		
		falsePos = 0;
		for(long i = ELEMENTS; i < 2 * ELEMENTS; ++i){
			if(newbf.test(String.valueOf(i))){
				++falsePos;
			}
		}
		
		System.out.println("falsePos = " + ((double)falsePos) / ELEMENTS);
		dis.close();
		newbf.clear();*/
	}
}