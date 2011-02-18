package jp.ndca.similarity.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import jp.ndca.similarity.join.Item;

/**
 * This implementation is based on  a letter : "Efficient Set Similarity Joins Using Min-Prefixes (2009)"</br>
 *
 * @author hattori_tsukasa
 *
 */
public class MPJoin extends AbstractSimilarityJoin{


	private NgramTokenizer tokenizer = new NgramTokenizer(2);

	private boolean useConvertingTypeData = false;

	private boolean useSortAtSearch = true;

	private boolean useSortAtExtractPairs = true;

	private boolean useSortAtExtractBulks = true;


	public NgramTokenizer getTokenizer()			{		return tokenizer;						}
	public boolean isUseConvertingTypeData()		{		return useConvertingTypeData;			}
	public boolean isUseSortAtSearch()				{		return useSortAtSearch;					}
	public boolean isUseSortAtExtractPairs()		{		return useSortAtExtractPairs;			}
	public boolean isUseSortAtExtractBulks()		{		return useSortAtExtractBulks;			}


	public void setTokenizer( NgramTokenizer tokenizer )
		{			this.tokenizer = tokenizer;				}
	public void setUseConvertingTypeData(boolean useConvertingTypeData)
		{		this.useConvertingTypeData = useConvertingTypeData;			}
	public void setUseSortAtSearch(boolean useSortAtSearch)
		{		this.useSortAtSearch = useSortAtSearch;						}
	public void setUseSortAtExtractPairs(boolean useSortAtExtractPairs)
		{		this.useSortAtExtractPairs = useSortAtExtractPairs;			}
	public void setUseSortAtExtractBulks(boolean useSortAtExtractBulks)
		{		this.useSortAtExtractBulks = useSortAtExtractBulks;			}


	/**
	 * convert a data type from String to Item.
	 *
	 * @param dataSet
	 * @return
	 */
	@Override
	public Item convert( String data, int id ) {
		return new Item( tokenizer.tokenize( data, useConvertingTypeData ), id );
	}


	/**
	 * convert a dataset type from String to Item.
	 *
	 * @param dataSet
	 * @return
	 */
	@Override
	public Item[] convert( List<String> dataSet ) {
		int len = dataSet.size();
		Item[] _dataSet = new Item[len];
		for( int i = 0 ; i < len ; i++ )
			_dataSet[i] = new Item( tokenizer.tokenize( dataSet.get(i), useConvertingTypeData ), i );
		Arrays.sort(_dataSet);
		return _dataSet;
	}


	/**
	 * extract similarity data-pairs in Item Type of dataSet.</br>
	 * This method extracts all similarity data-pairs with other than threshold.</br>
	 * And this is exact similarity search, but not approximate search.such as LSH </br>
	 *
	 * @param dataSet
	 * @param threshold
	 * @return
	 */
	@Override
	public List<Entry<Item,Item>> extractPairs( Item[] dataSet, double threshold ) {
		if(1 <= threshold)
			throw new IllegalArgumentException("argumenrt \"threshold\" is no less than 1.0");
		if( useSortAtExtractPairs )
			Arrays.sort( dataSet );
		return innerExtractPairs( dataSet, threshold );
	}

	class PrefixOverlap{
		int overlap;
		int i;
		int j;
	}


	/**
	 * MPJon core algorithm for "extractPairs" method</br>
	 *
	 * @param dataSet
	 * @param threshold
	 * @return
	 */
	private List<Entry<Item,Item>> innerExtractPairs( Item[] dataSet, double threshold ){

		double coff = threshold / ( 1 + threshold );

		List<Entry<Item,Item>> S = new ArrayList<Entry<Item,Item>>();
		int dataSetSize = dataSet.length;
		LinkedInvertedIndex index = new LinkedInvertedIndex();
		int[] prefixLengths = new int[dataSetSize];
		int[] minOrverlap	= new int[dataSetSize];
		PrefixOverlap[] M = new PrefixOverlap[dataSetSize];
		for(int i = 0 ; i < dataSetSize ; i++ )
			M[i] = new PrefixOverlap();

		for( int xDataSetID = 0 ; xDataSetID < dataSetSize ; xDataSetID++ ){

			Item x	= dataSet[xDataSetID];
			//refresh
			for( int i = 0 ; i < xDataSetID ; i++ ){
				M[i].i = 0;
				M[i].j = 0;
				M[i].overlap = 0;
			}

			int xSize = x.size();
			if( xSize ==  0 )
				continue;
			int maxPrefixLength = xSize - (int)Math.ceil( xSize * threshold ) + 1; // p : max-prefix-length
			for( int xPos = 0 ; xPos < maxPrefixLength ; xPos++ ){

				String w = x.get(xPos);
				LinkedPositions positions = index.get(w);
				if( positions != null ){

					LinkedPositions.Node node = positions.getRootNode();
					while( true ) {

						LinkedPositions.Node next = node.getNext();
						if( next == null )
							break;

						int yID	  = next.getId();
						int yPos  = next.getPosition();
						int ySize = dataSet[yID].size();

						// Jaccard constraint : Another constraint( xSize < ySize * threshold ) is never satisfied because of increasing order sort for dataset.
						// and not incrementing overlap means data pruning.
						if( ySize < xSize * threshold ){
							next.remove();
							continue;
						}

						// this is PrefixFilrtering,
						// Because not incrementing overlap means data pruning
						minOrverlap[yID] = (int)Math.ceil(  coff * ( ySize + xSize ) );
						int minPrefixLength = ySize - minOrverlap[yID] + 1;
						prefixLengths[yID] = minPrefixLength;
						if( prefixLengths[yID] < yPos + 1 ){
							next.remove();
							continue;
						}

						M[yID].overlap++;;
						M[yID].i = xPos;
						M[yID].j = yPos;
						int remx = xSize - xPos - 1;
						int remy = ySize - yPos - 1;
						int ubound = Math.min( remx, remy );
						if( M[yID].overlap + ubound < minOrverlap[yID] ) // Positional Filtering
							M[yID].overlap = Integer.MIN_VALUE;

						node = next;

					}

				}

			}
			veryfy( xDataSetID, dataSet, maxPrefixLength, M, prefixLengths, minOrverlap, S );
			int midPrefixLength = xSize - (int)Math.ceil( 2.0 / ( 1.0 + threshold ) * threshold  * xSize ) + 1; // mid-prefix-length
			prefixLengths[xDataSetID] = midPrefixLength;
			for( int xPos = 0 ; xPos < midPrefixLength ; xPos++ ){
				String w = x.get(xPos);
				index.put( w, xDataSetID, xPos );
			}

		}
		return S;

	}


	/**
	 * veryfy whether similarity of candidate data-pairs is over a threshold or not.</br>
	 * The similarity equals to Jaccard Similarity.</br>
	 * And This is used by "innerExtractPairs"</br>
	 *
	 * @param xDataSetID
	 * @param dataSet
	 * @param A
	 * @param prefixLengths
	 * @param alpha
	 * @param S
	 */
	private void veryfy( int xDataSetID, Item[] dataSet, int xMaxPrefixLength, PrefixOverlap[] M, int[] prefixLengths, int[] minOverlap, List<Entry<Item,Item>> S ){

		Item x = dataSet[xDataSetID];
		String wx_lastPrefix = x.get( xMaxPrefixLength - 1 );
		for( int yDataSetID = 0 ; yDataSetID < xDataSetID ; yDataSetID++ ){

			if( M[yDataSetID].overlap <= 0 )
				continue;

			Item y = dataSet[yDataSetID];
			String wy_lastPrefix = y.get( prefixLengths[yDataSetID]-1 );
			int xOffset = 0;
			int yOffset = 0;
			if( wx_lastPrefix.compareTo( wy_lastPrefix ) < 0 ){ // wx < wy
				xOffset = xMaxPrefixLength;
				yOffset = M[yDataSetID].j + 1;
			}
			else{
				xOffset = M[yDataSetID].i + 1;
				yOffset = prefixLengths[yDataSetID];
			}
			int overlapValue = overLap( x.getTokens(), xOffset, y.getTokens(), yOffset, M[yDataSetID].overlap, minOverlap[yDataSetID] );
			if( minOverlap[yDataSetID] <= overlapValue )
				S.add( new SimpleEntry<Item,Item>(x,y) );

		}

	}


	/**
	 * overlap accumulation with checking minOverlap constraint.
	 * @param x
	 * @param xOffSet
	 * @param y
	 * @param yOffSet
	 * @param defaultOverlap
	 * @param minOverlap
	 * @return
	 */
	private int overLap( String[] x, int xOffSet, String[] y, int yOffSet, int defaultOverlap, double minOverlap ){
		int overlap = defaultOverlap;
		int i = xOffSet;
		int j = yOffSet;
		while( i < x.length && j < y.length ){

			if( x[i].equals( y[j] ) ){
				overlap++;
				i++;
				j++;
			}
			else{

				if( x[i].compareTo(y[j]) < 0 ){
					if( overlap + ( x.length - i - 1 ) < minOverlap )
						break;
					i++;
				}
				else{
					if( overlap + ( y.length - j - 1 ) < minOverlap )
						break;
					j++;
				}
			}
		}
		return overlap;
	}


	@Override
	public List<Item> search( Item query, Item[] dataSet, double threshold ) {
		if( 1 < threshold )
			throw new IllegalArgumentException("argumenrt \"threshold\" is no less than 1.0");
		if( useSortAtSearch )
			Arrays.sort( dataSet );
		return innerSearch( query, dataSet, threshold );
	}


	/**
	 * MPJon core algorithm for "search" method</br>
	 *
	 * @param dataSet
	 * @param threshold
	 * @return
	 */
	private List<Item> innerSearch( Item x, Item[] dataSet, double threshold ){

		List<Item> S = new ArrayList<Item>();
		int dataSetSize = dataSet.length;
		int[] prefixLengths = new int[dataSetSize];
		int[] minOverlap = new int[dataSetSize];
		int xSize = x.size();
		if( xSize == 0 )
			return S;

		int xPrefixLength = xSize - (int)Math.ceil( xSize * threshold ) + 1; // p : max-prefix-length
		Set<String> xPrefixSet = new HashSet<String>();
		for( int xPos = 0 ; xPos < xPrefixLength ; xPos++ ){
			String w = x.get( xPos );
			xPrefixSet.add(w);
		}

		double coeff = threshold / (1+threshold);
		//InvertedIndexReadOnly index = new InvertedIndexReadOnly();
		LinkedInvertedIndex index = new LinkedInvertedIndex();
		for( int yID = 0 ; yID < dataSetSize ; yID++ ){
			Item y = dataSet[yID];
			int ySize = y.size();
			if( ySize == 0 )
				continue;
			// Jaccard constraint
			if( ySize < xSize * threshold || xSize < ySize * threshold )
				continue;

			minOverlap[yID]	= (int)Math.ceil( coeff * ( ySize + xSize ) );
			int yMinPrefixLength = ySize - minOverlap[yID] + 1; // min-prefix-length
			prefixLengths[yID] = yMinPrefixLength;
			for( int yPos = 0 ; yPos < yMinPrefixLength ; yPos++ ){
				String w = y.get(yPos);
				if( xPrefixSet.contains(w) )
					index.put(w, yID, yPos);
			}
		}

		PrefixOverlap[] M = new PrefixOverlap[dataSetSize];
		for( int i = 0 ; i < dataSetSize ; i++ )
			M[i] = new PrefixOverlap();

		for( int xPos = 0 ; xPos < xPrefixLength ; xPos++ ){
			String w = x.get(xPos);
			LinkedPositions positions = index.get(w);
			if( positions != null ){
				LinkedPositions.Node node = positions.getRootNode();
				while( true ){
					LinkedPositions.Node next = node.getNext();
					if( next == null )
						break;
					int yID = next.getId();
					if( M[yID].overlap == Integer.MIN_VALUE ){
						next.remove();
						continue;
					}
					Item y	  = dataSet[yID];
					int ySize = y.size();
					int yPos  = node.getPosition();

					// this point Jaccard Constraint is already satissfied !
					int unbound = Math.min( xSize - xPos , ySize - yPos ) - 1;
					M[yID].overlap++;
					M[yID].i = xPos;
					M[yID].j = yPos;
					if( M[yID].overlap + unbound < minOverlap[yID] )
						M[yID].overlap = Integer.MIN_VALUE;
					node = next;
				}
			}
		}
		veryfy( x, xPrefixLength, dataSet, M, prefixLengths, minOverlap, S );
		return S;

	}

	/**
	 * veryfy whether similarity of candidate data-pairs is over a threshold or not.</br>
	 * The similarity equals to Jaccard Similarity.</br>
	 * And This is used by "innerSearch"</br>
	 *
	 * @param xDataSetID
	 * @param dataSet
	 * @param A
	 * @param prefixLengths
	 * @param alpha
	 * @param S
	 */
	private void veryfy( Item x, int xMaxPrefixLength, Item[] dataSet, PrefixOverlap[] M, int[] prefixLengths, int[] minOverlap, Collection<Item> S ){
		String wx_lastPrefix = x.get( xMaxPrefixLength - 1 );
		for( int yID = 0 ; yID < dataSet.length ; yID++ ){

			if( M[yID].overlap <= 0 )
				continue;

			Item y = dataSet[yID];
			String wy_lastPrefix = y.get( prefixLengths[yID]-1 );
			int xOffset = 0;
			int yOffset = 0;
			if( wx_lastPrefix.compareTo( wy_lastPrefix ) < 0 ){ // wx < wy
				xOffset = xMaxPrefixLength;
				yOffset = M[yID].j + 1;
			}
			else{
				xOffset = M[yID].i + 1;
				yOffset = prefixLengths[yID];
			}
			int overlapValue = overLap( x.getTokens(), xOffset, y.getTokens(), yOffset, M[yID].overlap, minOverlap[yID] );

			if( minOverlap[yID] <= overlapValue )
				S.add( y );

		}
	}



	@Override
	public List<List<Item>> extractBulks(Item[] dataSet, double threshold) {
		if(1 <= threshold)
			throw new IllegalArgumentException("argumenrt \"threshold\" is no less than 1.0");
		if( useSortAtExtractBulks )
			Arrays.sort( dataSet );
		return innerExtractBulks( dataSet, threshold );
	}


	/**
	 * MPJon core algorithm for "extractBulks" method</br>
	 *
	 * @param dataSet
	 * @param threshold
	 * @return
	 */
	private List<List<Item>> innerExtractBulks( Item[] dataSet, double threshold ){

		double coff = threshold / ( 1 + threshold );

		Set<Integer> buffer = new HashSet<Integer>();
		List<List<Item>> result = new ArrayList<List<Item>>();

		int dataSetSize = dataSet.length;
		LinkedInvertedIndex index = new LinkedInvertedIndex();
		int[] prefixLengths = new int[dataSetSize];
		int[] minOrverlap	= new int[dataSetSize];
		PrefixOverlap[] M = new PrefixOverlap[dataSetSize];
		for(int i = 0 ; i < dataSetSize ; i++ )
			M[i] = new PrefixOverlap();


		for( int xDataSetID = 0 ; xDataSetID < dataSetSize ; xDataSetID++ ){

			Item x = dataSet[xDataSetID];
			int xSize = x.size();
			if( xSize == 0 ){
				buffer.add( xDataSetID );
				continue;
			}

			//refresh
			for( int i = 0 ; i < xDataSetID ; i++ ){
				M[i].i = 0;
				M[i].j = 0;
				M[i].overlap = 0;
			}

			int maxPrefixLength = xSize - (int)Math.ceil( xSize * threshold ) + 1; // p : max-prefix-length
			for( int xPos = 0 ; xPos < maxPrefixLength ; xPos++ ){

				String w = x.get(xPos);
				LinkedPositions positions = index.get(w);
				if( positions != null ){

					LinkedPositions.Node node = positions.getRootNode();
					while( true ) {

						LinkedPositions.Node next = node.getNext();
						if( next == null )
							break;

						int yID	= next.getId();
						if( buffer.contains( yID ) ){
							next.remove();
							continue;
						}
						int yPos  = next.getPosition();
						int ySize = dataSet[yID].size();

						// Jaccard constraint , Another constraint( xSize < ySize * threshold ) is never satisfied because of increasing order sort for dataset.
						if( ySize < xSize * threshold ){
							next.remove();
							continue;
						}

						minOrverlap[yID] = (int)Math.ceil(  coff * ( ySize + xSize ) );
						int minPrefixLength = ySize - minOrverlap[yID] + 1;
						prefixLengths[yID] = minPrefixLength;
						if( prefixLengths[yID] < yPos + 1 ){
							next.remove();
							continue;
						}

						M[yID].overlap++;;
						M[yID].i = xPos;
						M[yID].j = yPos;
						int remx = xSize - xPos - 1;
						int remy = ySize - yPos - 1;
						int ubound = Math.min( remx, remy );
						if( M[yID].overlap + ubound < minOrverlap[yID] )
							M[yID].overlap = Integer.MIN_VALUE;

						node = next;

					}

				}

			}
			List<Item> S = new ArrayList<Item>();
			veryfy( xDataSetID, dataSet, maxPrefixLength, M, prefixLengths, minOrverlap, S, buffer );
			if( 0 < S.size() ){
				buffer.add( x.getId() );
				S.add(x);
				boolean isUnioned = union( S, result, threshold, buffer );
				if( !isUnioned )
					result.add(S);
			}
			else{
				S.add(x);
				boolean isUnioned = union( S, result, threshold, buffer );
				if( !isUnioned ){
					int midPrefixLength = xSize - (int)Math.ceil( 2.0 * coff * xSize ) + 1; // mid-prefix-length
					prefixLengths[xDataSetID] = midPrefixLength;
					for( int xPos = 0 ; xPos < midPrefixLength ; xPos++ ){
						String w = x.get(xPos);
						index.put( w, xDataSetID, xPos );
					}
				}
			}

		}
		Collections.sort( result, new Comparator<List<Item>>(){

			@Override
			public int compare(List<Item> o1, List<Item> o2) {
				int size1 = o1.size();
				int size2 = o2.size();
				if( size1 < size2 )
					return  1;
				else if( size2 < size1 )
					return -1;
				return 0;
			}

		} );
		return result;

	}


	/**
	 * veryfy whether similarity of candidate data-pairs is over a threshold or not.</br>
	 * The similarity equals to Jaccard Similarity.</br>
	 * And This is used by "innerExtractBulks"</br>
	 * @param xDataSetID
	 * @param dataSet
	 * @param xMaxPrefixLength
	 * @param M
	 * @param prefixLengths
	 * @param minOverlap
	 * @param S
	 * @param buffer
	 */
	private void veryfy( int xDataSetID, Item[] dataSet, int xMaxPrefixLength, PrefixOverlap[] M, int[] prefixLengths, int[] minOverlap, List<Item> S, Set<Integer> buffer ){

		Item x = dataSet[xDataSetID];
		String wx_lastPrefix = x.get( xMaxPrefixLength - 1 );
		for( int yDataSetID = 0 ; yDataSetID < xDataSetID ; yDataSetID++ ){

			if( M[yDataSetID].overlap <= 0 )
				continue;

			Item y = dataSet[yDataSetID];
			String wy_lastPrefix = y.get( prefixLengths[yDataSetID]-1 );
			int xOffset = 0;
			int yOffset = 0;
			if( wx_lastPrefix.compareTo( wy_lastPrefix ) < 0 ){ // wx < wy
				xOffset = xMaxPrefixLength;
				yOffset = M[yDataSetID].j + 1;
			}
			else{
				xOffset = M[yDataSetID].i + 1;
				yOffset = prefixLengths[yDataSetID];
			}
			int overlapValue = overLap( x.getTokens(), xOffset, y.getTokens(), yOffset, M[yDataSetID].overlap, minOverlap[yDataSetID] );
			if( minOverlap[yDataSetID] <= overlapValue ){
				S.add(y);
				buffer.add( yDataSetID );
			}

		}

	}

}