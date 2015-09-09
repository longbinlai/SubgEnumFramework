package dbg.hadoop.subgraphs.utils;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import dbg.hadoop.subgraphs.io.HVArray;

public class Test{
	public static void main(String[] args) 
			throws InstantiationException, IllegalAccessException, IOException{
		
		SecureRandom rand = new SecureRandom();
		System.out.println(rand.nextLong());
		
	}
}