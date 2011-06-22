package jp.ndca.similarity.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class MPJoinTest {

	static String encode = "utf-8";

	static double threshold = 0.90;

	static MPJoin join;

	{
		join = new MPJoin();
	}

	@Test
	public void extractPairsTest() throws IOException{

		int id = 0;
		List<IntItem> intItems = new ArrayList<IntItem>();
		List<StringItem> stringItems = new ArrayList<StringItem>();
		List<String> strDataSet	= new ArrayList<String>();

		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("lsh.txt");
		BufferedReader br = new BufferedReader( new InputStreamReader(is, encode) );
		while( br.ready() ){

			String line = br.readLine();
			line = line.substring( 1, line.length()-1 );
			if( line.length() < 1 ){
				System.out.println("skip");
				continue;
			}
			String[] strs = line.split(",");
			IntList intTokens = new IntArrayList();
			List<String> strTokens = new ArrayList<String>();
			for( String str : strs ){
				strTokens.add( str.trim() );
				intTokens.add( Integer.parseInt( str.trim() ) );
			}
			if( strTokens.size() < 10 )
				continue;

			String[] strArrays = strTokens.toArray( new String[strTokens.size()] );
			int[] intArrays = intTokens.toArray( new int[intTokens.size()] );
			Arrays.sort(strArrays);
			intItems.add( join.intConvert(intArrays, id) );
			stringItems.add( new StringItem(strArrays, id) );
			strDataSet.add(line);
			id++;
		}
		br.close();

		IntItem[] intDatum = intItems.toArray( new IntItem[intItems.size()] );
		StringItem[] strDatum = stringItems.toArray( new StringItem[stringItems.size()] );
		Arrays.sort(strDatum);
		Arrays.sort(intDatum);
		
		join.setUseSortAtExtractPairs(false);

		long start = System.currentTimeMillis();
		List<Entry<StringItem,StringItem>> result = join.extractPairs( strDatum, threshold );
		long end = System.currentTimeMillis();
		long diff = end - start;
		System.out.println( "dataSize : " + stringItems.size() );
		System.out.println( "result pairs: " + result.size() );
		System.out.println( "calc time : " + diff );
		
		start = System.currentTimeMillis();
		List<Entry<IntItem,IntItem>> _result = join.extractPairs( intDatum, threshold );
		end = System.currentTimeMillis();
		diff = end - start;
		System.out.println( "dataSize : " + stringItems.size() );
		System.out.println( "result pairs: " + _result.size() );
		System.out.println( "calc time : " + diff );
		

	}
}
