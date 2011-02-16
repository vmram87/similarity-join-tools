package jp.ndca.similarity.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Test;

public class MPJoinTest {

	static String encode = "utf-8";

	static double threshold = 0.95;

	static MPJoin join;

	{
		join = new MPJoin();
	}

	@Test
	public void extractPairsTest() throws IOException{

		int id = 0;
		List<Item> dataSet = new ArrayList<Item>();
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
			List<String> tokens = new ArrayList<String>();
			for( String str : strs )
				tokens.add( str.trim() );
			if( tokens.size() < 10 )
				continue;

			String[] arrays = tokens.toArray( new String[tokens.size()] );
			Arrays.sort(arrays);
			dataSet.add( new Item(arrays, id++) );
			strDataSet.add(line);

		}
		br.close();

		Item[] datum = dataSet.toArray( new Item[dataSet.size()] );
		Arrays.sort(datum);

		join.setUseSortAtExtractPairs(false);

		long start = System.currentTimeMillis();
		List<Entry<Item,Item>> result = join.extractPairs( datum, threshold );
		long end = System.currentTimeMillis();

		long diff = end - start;
		PrintWriter pw = new PrintWriter("C:\\similarity_pairs_mp.txt");

		Collections.sort(result, new Comparator<Entry<Item,Item>>(){

			@Override
			public int compare(Entry<Item, Item> o1, Entry<Item, Item> o2) {

				int x1 = o1.getKey().getId();
				int x2 = o1.getValue().getId();
				int xMax = Math.max(x1, x2);
				int xMin = Math.min(x1, x2);

				int y1 = o2.getKey().getId();
				int y2 = o2.getValue().getId();
				int yMax = Math.max(y1, y2);
				int yMin = Math.min(y1, y2);
				if( xMin < yMin )
					return -1;
				if( yMin < xMin )
					return  1;
				if( xMax < yMax )
					return  -1;
				if( yMax < xMax )
					return   1;
				return 0;

			}


		});

		for( Entry<Item,Item> entry : result ){
			int keyID = entry.getKey().getId() ;
			int valueID = entry.getValue().getId() ;
			int min = Math.min(keyID, valueID);
			int max = Math.max(keyID, valueID);
			pw.println( min + ":" + max );
		}
		pw.close();

		pw = new PrintWriter("C:\\lsh_data.txt");
		for( int i = 0 ; i < strDataSet.size() ; i++)
			pw.println( i + ":" + strDataSet.get(i) );
		pw.close();

		System.out.println( "dataSize : " + dataSet.size() );
		System.out.println( "result pairs: " + result.size() );
		System.out.println( "calc time : " + diff );
	}
}
