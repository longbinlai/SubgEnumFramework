package dbg.hadoop.subgraphs.utils;

public class Hash{
	private static long p = 2147483647;
	//private static int seed = 1234;
	//private static long a = 1388524629;
	//private static long b = 557894633;
	
	private static int base = 10;
	
	public static int hashValue(String input){
		long res = input.hashCode();
		res %= p;
		if(res < 0){
			res += p;
		}
		return (int)(res % base);
	}
	
	public static int hashValue(int input){
		long res = Integer.toString(input).hashCode();
		res %= p;
		if(res < 0){
			res += p;
		}
		return (int)(res % base);
	}
	
	// We want base at least be 2
	public static void setBase(int b){
		base = b;
		if(base == 1){
			base += 1;
		}
	}
	
	public static int getBase(){
		return base;
	}
}