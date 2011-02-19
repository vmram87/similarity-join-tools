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

import jp.ndca.similarity.distance.Overlap;
import jp.ndca.similarity.join.Item;

/**
 * This implementation is based on  a letter : "Efficient Similarity Joins for Near Duplicate Detection. (2008)"</br>
 *
 * @author hattori_tsukasa
 *
 */
public class PPJoin extends AbstractSimilarityJoin{

	private static final int DEFAULT_MAX_DEPTH = 3;

	private NgramTokenizer tokenizer = new NgramTokenizer(2);

	private boolean usePlus		= false;

	private boolean useConvertingTypeData = false;

	private boolean useSortAtSearch = true;

	private boolean useSortAtExtractPairs = true;

	private boolean useSortAtExtractBulks = true;

	private int maxDepth = DEFAULT_MAX_DEPTH;

	private  static final Overlap overlap = new Overlap();


	public NgramTokenizer getTokenizer()			{		return tokenizer;			}
	public boolean isUsePlus()						{		return usePlus;				}
	public int getMaxDepth()						{		return maxDepth;			}
	public boolean isUseConvertingTypeData()		{		return useConvertingTypeData;			}
	public boolean isUseSortAtSearch()				{		return useSortAtSearch;					}
	public boolean isUseSortAtExtractPairs()		{		return useSortAtExtractPairs;			}
	public boolean isUseSortAtExtractBulks()		{		return useSortAtExtractBulks;			}


	public void setTokenizer( NgramTokenizer tokenizer )
		{		this.tokenizer = tokenizer;			}
	public void setUsePlus( boolean usePlus )
		{		this.usePlus = usePlus;				}
	public void setMaxDepth( int maxDepth )
		{		this.maxDepth = maxDepth;			}
	public void setUseConvertingTypeData(boolean useConvertingTypeData)
		{		this.useConvertingTypeData = useConvertingTypeData;		}
	public void setUseSortAtSearch(boolean useSortAtSearch)
		{		this.useSortAtSearch = useSortAtSearch;					}
	public void setUseSortAtExtractPairs(boolean useSortAtExtractPairs)
		{		this.useSortAtExtractPairs = useSortAtExtractPairs;					}
	public void setUseSortAtExtractBulks(boolean useSortAtExtractBulks)
		{		this.useSortAtExtractBulks = useSortAtExtractBulks;					}


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
		if(useSortAtExtractPairs)
			Arrays.sort( dataSet );
		return innerExtractPairs( dataSet, threshold );
	}


	/**
	 * PPJon and PPjoin+ core algorithm for "extractPairs" method</br>
	 *
	 * @param dataSet
	 * @param threshold
	 * @return
	 */
	private List<Entry<Item,Item>> innerExtractPairs( Item[] dataSet, double threshold ){

		double coeff = threshold / ( 1 + threshold );

		List<Entry<Item,Item>> S = new ArrayList<Entry<Item,Item>>();
		LinkedInvertedIndex index = new LinkedInvertedIndex();

		int dataSetSize = dataSet.length;
		int[] prefixLengths = new int[dataSetSize];
		int[] alpha = new int[dataSetSize];
		for( int xDataSetID = 0 ; xDataSetID < dataSetSize ; xDataSetID++ ){

			Item x = dataSet[xDataSetID];
			int[] A	= new int[xDataSetID];
			int xSize = x.size();
			if( xSize ==  0 )
				continue;
			int maxPrefixLength = xSize - (int)Math.ceil( xSize * threshold ) + 1; // p : max-prefix-length

			if( usePlus )

				for( int xPos = 0 ; xPos < maxPrefixLength ; xPos++ ){

					String w = x.get(xPos);
					LinkedPositions positions = index.get(w);
					if( positions != null ){

						LinkedPositions.Node node = positions.getRootNode();
						while( true ) {

							LinkedPositions.Node next = node.getNext();
							if( next == null )
								break;

							int yID	  =	next.getId();
							// alpha : lower Overlap
							if( A[yID] == Integer.MIN_VALUE ){
								node = next;
								continue;
							}

							Item y	  =	dataSet[yID];
							int yPos  =	next.getPosition();
							int ySize =	y.size();

							// Jaccard constraint : another condition:"xSize < ySize * threshold" is not satisfied due to increasing ordering for dataSet.
							if( ySize < xSize * threshold ){
								next.remove();
								continue;
							}

							alpha[yID] = (int)Math.ceil( coeff * ( ySize + xSize ) );
							A[yID]++;
							// arugumnet taht global oerdered x and y has same sequence after *Pos
							// ubound don't needs '+1' because of xPos is pointer from id=0 ;
							int ubound = Math.min( xSize - xPos, ySize - yPos ) -1;
							if( A[yID] + ubound < alpha[yID] )
								A[yID] = Integer.MIN_VALUE;
							else{

								// execute in only first phase!
								if( A[yID] == 1 ){
									// Hamming Distance Constraint : Hamming distance between part of x and y  after *Pos must exceed hmax.
									// h' <= hmax + ( xPos + 1 + yPos +1 ) - 2 = |x| + |y| - 2α = |x| + |y| - 2t / ( 1 + t )
									int hmax = xSize + ySize - 2 * (int)Math.ceil( coeff * ( ySize + xSize ) ) - ( xPos + yPos ); // ubound don't needs '+2' because of xPos is pointer from id=0 ;
									int h = suffixFilter( x.getTokens(), xPos+1, x.size()-xPos-1, y.getTokens(), yPos+1, y.size()-yPos-1, hmax, 0 );
									if( hmax < h )
										A[yID] = Integer.MIN_VALUE;
								}

							}
							node = next;
						}

					}
				}

			else{

				for( int xPos = 0 ; xPos < maxPrefixLength ; xPos++ ){
					String w = x.get(xPos);

					LinkedPositions positions = index.get(w);
					if( positions != null ){

						LinkedPositions.Node node = positions.getRootNode();
						while( true ) { // positionSet don't have components with the same position.id

							LinkedPositions.Node next = node.getNext();
							if( next == null )
								break;

							int yID   = next.getId();

							if( A[yID] == Integer.MIN_VALUE ){
								node = next;
								continue;
							}

							Item y	  = dataSet[yID];
							int yPos  = next.getPosition();
							int ySize = y.size();
							if( ySize < xSize * threshold ){ // Jaccard constraint , and another constraint( xSize < ySize * threshold ) is already satisfied.
								next.remove();
								continue;
							}

							alpha[yID]	= (int)Math.ceil( coeff  * ( ySize + xSize ) );
							A[yID]++;
							int ubound = Math.min( xSize - xPos, ySize - yPos ) - 1;
							if( A[yID] + ubound < alpha[yID] )
								A[yID] = Integer.MIN_VALUE;
							node = next;
						}
					}
				}

			}
			int midPrefixLength = xSize - (int)Math.ceil( 2.0 * coeff * xSize ) + 1; // mid-prefix-length
			prefixLengths[xDataSetID] = midPrefixLength;
			veryfy( xDataSetID, dataSet, maxPrefixLength, A, prefixLengths, alpha, S );
			for( int xPos = 0 ; xPos < midPrefixLength ; xPos++ ){
				String w = x.get(xPos);
				index.put( w, xDataSetID, xPos );
			}

		}
		return S;

	}


	/**
	 * SuffixFilter :</br>
	 * 	Two tokens, x and y, are compared by HammingDistance constraint.</br>
	 *  which  takes account of admitable bound.</br>
	 *
	 * @param x		 : tokens
	 * @param xStart : first index of searched x.
	 * @param xEnd	 : last index of searched x.
	 * @param y		 : tokens
	 * @param yStart : first index of searched y.
	 * @param yEnd	 : last index of searched y.
	 * @param hmax	 : max Hamming Distance
	 * @param d		 : current Depth
	 * @return		 : lower Hamming Distance between x and y ranged from *Start to *End
	 */
	private int suffixFilter( String[] x, int xPos, int xLen, String[] y, int yPos, int yLen, int hmax, int d ){

		int ol, or;
		if( maxDepth <= d || yLen == 0 || xLen == 0)
			return Math.abs( yLen - xLen );

		int halfLength = (int)Math.ceil( 0.5 * yLen );
		int ymid  = yPos + halfLength - 1;

		String wy = y[ymid];

		int o  = (int)Math.ceil( 0.5 * ( hmax - Math.abs(yLen - xLen) ) );
		if( xLen < yLen ){
			ol = 1;
			or = 0;
		}
		else{
			ol = 0;
			or = 1;
		}

		int ylPos = yPos;
		int ylLen = halfLength - 1;

		int rLength = yLen - ylLen -1;
		int yrPos = ymid + 1;
		int yrLen = rLength;

		Partition xPartition = partition( x, xPos, xLen, wy, (xPos+halfLength-1)  - o - Math.abs(yLen - xLen) * ol, (xPos+halfLength-1) + o + Math.abs(yLen - xLen) * or );

		int f = xPartition.isRange;
		int diff = xPartition.notFind;

		int xlLen = xPartition.slLen;
		int xlPos = xPartition.slPos;
		int xrLen = xPartition.srLen;
		int xrPos = xPartition.srPos;


		if( f == 0 ) // exist wy in x.
			return ++hmax;
		int h = Math.abs( xlLen - ylLen ) + Math.abs( xrLen - yrLen ) + diff;
		if( hmax < h )
			return h;
		else{

			int next_d = d + 1;

			int hl = suffixFilter( x, xlPos, xlLen, y, ylPos, ylLen, hmax - Math.abs( xrLen - yrLen ) - diff, next_d );
			h = hl + Math.abs( xrLen - yrLen ) + diff;
			if( hmax < h )
				return h;
			else {
				int hr = suffixFilter( x, xrPos, xrLen, y, yrPos, yrLen, hmax - hl - diff, next_d );
				return hr + hl + diff;
			}

		}
	}


	/**
	 * Partition:</br>
	 * 	intersection with word 'w' into two letter string.</br>
	 *
	 * @param x		: item
	 * @param start	: start bound of item
	 * @param end	: end bound of item
	 * @param w		: used-parition word
	 * @param l		: search lower point
	 * @param r		: search upper point
	 * @return
	 */
	private Partition partition( String[] x, int xPos, int xLen, String w, int l, int r ){

		int lastIndex = xPos + xLen - 1;

		if( l < xPos )	l = xPos;
		if( lastIndex < r )	r = lastIndex;

		String wl = x[l];
		String wr = x[r];

		if( w.compareTo(wl) < 0 || wr.compareTo(w) < 0){
			Partition p = new Partition();
			p.slPos = xPos;
			p.slLen = 0;
			p.srPos = xPos;
			p.srLen = 0;
			p.isRange = 0;
			p.notFind = 1;
			return p;
		}
		binarySearch( x, w, l, r );

		Partition p = new Partition();

		int partioningPoint = box.position;

		int slLen = partioningPoint - xPos;
		int slPos = xPos;

		int srPos,srLen;
		//if( x[partioningPoint].equals(w) ){
		if( box.isfound ){
			srLen = xLen - slLen -1;
			if( srLen < 0 )
				srLen= 0;
			srPos = partioningPoint+1;
			p.notFind = 0;
		}
		else{
			srLen = xLen - slLen;
			srPos = partioningPoint;
			p.notFind = 1;
		}
		p.isRange = 1;
		p.slLen = slLen;
		p.srLen = srLen;
		p.slPos = slPos;
		p.srPos = srPos;
		return p;

	}

	private SearchBox box = new SearchBox();

	class SearchBox{
		boolean isfound;
		int position;
	}

	private void binarySearch( String[] x, String query, int start, int end ){
		while( start < end ){
			int midd = (int)( 0.5 * ( start + end ) );
			String w = x[midd];
			if( query.compareTo( w ) == 0 ){
				box.isfound = true;
				box.position = midd;
				return;
			}
			else if( query.compareTo( w ) < 0 )
				end = --midd;
			else
				start = ++midd;
		}
		String w = x[end];
		if( query.compareTo( w ) == 0 ){
			box.isfound = true;
			box.position = end;
			return;
		}
		else if( query.compareTo( w ) < 0 ){
			box.isfound = false;
			box.position = --end;
			return;
		}
		else{
			box.isfound = false;
			box.position = ++end;
			return;
		}
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
	private void veryfy( int xDataSetID, Item[] dataSet, int maxXPrefixLength, int[] A, int[] prefixLengths, int[] alpha, List<Entry<Item,Item>> S ){

		Item x = dataSet[xDataSetID];
		String wx_lastPrefix = x.get( maxXPrefixLength-1 );
		for( int yDataSetID = 0 ; yDataSetID < xDataSetID ; yDataSetID++ ){

			if( A[yDataSetID] <= 0 )
				continue;

			int overlapValue = A[yDataSetID];
			Item y = dataSet[yDataSetID];
			String wy_lastPrefix = y.get( prefixLengths[yDataSetID]-1 );
			if( wx_lastPrefix.compareTo( wy_lastPrefix ) < 0 ){ // wx < wy
				int unbound = A[yDataSetID] + x.size() - maxXPrefixLength;
				if( alpha[yDataSetID] <= unbound )
					overlapValue += overlap.calcByMerge( x.getTokens(), maxXPrefixLength, y.getTokens(), A[yDataSetID] );
			}
			else{
				int unbound = A[yDataSetID] + y.size() - prefixLengths[yDataSetID];
				if( alpha[yDataSetID] <= unbound )
					overlapValue += overlap.calcByMerge( x.getTokens(), A[yDataSetID], y.getTokens(), prefixLengths[yDataSetID] );
			}

			if( alpha[yDataSetID] <= overlapValue )
				S.add( new SimpleEntry<Item,Item>(x,y) );

		}

	}

	/**
	 * search datum with similarity to query  from Item Type of dataSet.</br>
	 * This method extracts all similarity datum with other than threshold.</br>
	 * And this is exact similarity search, but not approximate search.such as LSH </br>
	 *
	 * @param query
	 * @param dataSet
	 * @param threshold
	 * @return
	 */
	@Override
	public List<Item> search( Item query, Item[] dataSet, double threshold ) {
		if( 1 <= threshold )
			throw new IllegalArgumentException("argumenrt \"threshold\" is no less than 1.0");
		if( useSortAtSearch )
			Arrays.sort( dataSet );
		return innerSearch( query, dataSet, threshold );
	}


	/**
	 * PPJon and PPjoin+ core algorithm for "search" method</br>
	 *
	 * @param dataSet
	 * @param threshold
	 * @return
	 */
	private List<Item> innerSearch( Item x, Item[] dataSet, double threshold ){

		double coeff = threshold / (1+threshold);

		List<Item> S = new ArrayList<Item>();
		int dataSetSize = dataSet.length;
		int[] prefixLengths = new int[dataSetSize];
		int[] alpha = new int[dataSetSize];
		int xSize = x.size();
		if( xSize == 0 )
			return S;

		int xPrefixLength = xSize - (int)Math.ceil( xSize * threshold ) + 1; // p : max-prefix-length
		Set<String> xPrefixSet = new HashSet<String>();
		for( int xPos = 0 ; xPos < xPrefixLength ; xPos++ ){
			String w = x.get( xPos );
			xPrefixSet.add(w);
		}

		LinkedInvertedIndex index = new LinkedInvertedIndex();
		for( int dataSetID = 0 ; dataSetID < dataSetSize ; dataSetID++ ){
			Item y = dataSet[dataSetID];
			int ySize = y.size();
			if( ySize == 0 )
				continue;
			if( ySize < xSize * threshold || xSize < ySize * threshold )// Jaccard constraint
				continue;
			int yPrefixLength = ySize - (int)Math.ceil( coeff * ( ySize + xSize ) ) + 1; // min-prefix-length
			prefixLengths[dataSetID] = yPrefixLength;
			for( int yPos = 0 ; yPos < yPrefixLength ; yPos++ ){
				String w = y.get(yPos);
				if( xPrefixSet.contains(w) )
					index.put(w, dataSetID, yPos);
			}
		}

		int[] A	= new int[dataSetSize];
		if( usePlus )
			for( int xPos = 0 ; xPos < xPrefixLength ; xPos++ ){
				String w = x.get(xPos);
				LinkedPositions positions = index.get(w);
				if( positions != null ){
					LinkedPositions.Node node = positions.getRootNode();
					while( true ) {

						LinkedPositions.Node next = node.getNext();
						if( next == null )
							break;

						int yID   = next.getId();
						if( A[yID] == Integer.MIN_VALUE ){
							node = next;
							continue;
						}

						Item y	  = dataSet[yID];
						int yPos  = next.getPosition();
						int ySize = y.size();
						// this point Jaccard Constraint is already satissfied !
						alpha[yID]	= (int)Math.ceil(  coeff  * ( ySize + xSize ) );
						A[yID]++;
						int unbound = Math.min( xSize - xPos, ySize - yPos ) - 1;
						if( A[yID] + unbound < alpha[yID] )
							A[yID] = Integer.MIN_VALUE;
						else{
							// first !
							if( A[yID] == 1 ){
								int hmax = xSize + ySize - 2 * (int)Math.ceil( coeff * ( ySize + xSize ) ) - ( xPos + yPos ); // ubound don't needs '+2' because of xPos is pointer from id=0 ;
								int h = suffixFilter( x.getTokens(), xPos+1, xSize-xPos-1, y.getTokens(), yPos+1, ySize-yPos-1, hmax, 0 );
								if( hmax < h )
									A[yID] = Integer.MIN_VALUE;
							}
						}
						node = next;
					}
				}
			}
		else{

			for( int xPos = 0 ; xPos < xPrefixLength ; xPos++ ){

				String w = x.get(xPos);
				LinkedPositions positions = index.get(w);
				if( positions != null ){
					LinkedPositions.Node node = positions.getRootNode();
					while( true ) {

						LinkedPositions.Node next = node.getNext();
						if( next == null )
							break;

						int yID   = next.getId();
						if(A[yID] == Integer.MIN_VALUE){
							node = next;
							continue;
						}

						Item y	  = dataSet[yID];
						int yPos  = next.getPosition();
						int ySize = y.size();
						// this point Jaccard Constraint is already satissfied !
						alpha[yID]	= (int)Math.ceil( coeff * ( ySize + xSize ) );
						A[yID]++;
						int unbound = Math.min( xSize - xPos, ySize - yPos ) - 1;
						if( A[yID] + unbound < alpha[yID] )
							A[yID] = Integer.MIN_VALUE;
						node = next;
					}
				}
			}

		}
		veryfy( x, xPrefixLength, dataSet, A, prefixLengths, alpha, S );
		return S;

	}


	/**
	 * veryfy whether similarity between query:x and candidate data is over a threshold or not.</br>
	 * The similarity equals to Jaccard Similarity.</br>
	 * And This is used by "innerSearch"</br>
	 *
	 * @param x
	 * @param xPrefixLengths
	 * @param dataSet
	 * @param A
	 * @param prefixLengths
	 * @param alpha
	 * @param S
	 */
	private void veryfy( Item x, int xPrefixLengths, Item[] dataSet, int[] A, int[] prefixLengths, int[] alpha, Collection<Item> S ){

		String wx_lastPrefix = x.get( xPrefixLengths-1 );
		for( int yDataSetID = 0 ; yDataSetID < A.length ; yDataSetID++ ){

			if( A[yDataSetID] <= 0 )
				continue;

			int overlapValue = A[yDataSetID];
			Item y = dataSet[yDataSetID];
			String wy_lastPrefix = y.get( prefixLengths[yDataSetID]-1 );
			if( wx_lastPrefix.compareTo( wy_lastPrefix ) < 0 ){ // wx < wy
				int unbound = A[yDataSetID] + x.size() - xPrefixLengths;
				if( alpha[yDataSetID] <= unbound )
					overlapValue += overlap.calcByMerge( x.getTokens(), xPrefixLengths, y.getTokens(), A[yDataSetID] );
			}
			else{
				int unbound = A[yDataSetID] + y.size() - prefixLengths[yDataSetID];
				if( alpha[yDataSetID] <= unbound )
					overlapValue += overlap.calcByMerge( x.getTokens(), A[yDataSetID], y.getTokens(), prefixLengths[yDataSetID] );
			}

			if( alpha[yDataSetID] <= overlapValue )
				S.add( y );
		}

	}


	@Override
	public List<List<Item>> extractBulks( Item[] dataSet, double threshold ) {
		if(1 <= threshold)
			throw new IllegalArgumentException("argumenrt \"threshold\" is no less than 1.0");
		if( useSortAtExtractBulks )
			Arrays.sort( dataSet );
		return innerExtractBulks( dataSet, threshold );
	}


	/**
	 * PPJon and PPjoin+ core algorithm for "ExtractBulks" method</br>
	 *
	 * @param dataSet
	 * @param threshold
	 * @return
	 */
	private List<List<Item>> innerExtractBulks( Item[] dataSet, double threshold ){

		double coff = threshold / ( 1 + threshold );

		Set<Integer> buffer = new HashSet<Integer>();
		List<List<Item>> result = new ArrayList<List<Item>>();

		LinkedInvertedIndex index = new LinkedInvertedIndex();
		int dataSetSize = dataSet.length;
		int[] prefixLengths = new int[dataSetSize];
		int[] alpha = new int[dataSetSize];
		for( int xDataSetID = 0 ; xDataSetID < dataSetSize ; xDataSetID++ ){

			if( buffer.contains(xDataSetID) )
				continue;

			Item x = dataSet[xDataSetID];
			int[] A	= new int[xDataSetID];
			int xSize = x.size();
			if( xSize ==  0 ){
				buffer.add(xDataSetID);
				continue;
			}
			int maxPrefixLength = xSize - (int)Math.ceil( xSize * threshold ) + 1; // p : max-prefix-length

			if( usePlus )

				for( int xPos = 0 ; xPos < maxPrefixLength ; xPos++ ){

					String w = x.get(xPos);
					LinkedPositions positions = index.get(w);
					if( positions != null ){

						LinkedPositions.Node node = positions.getRootNode();
						while( true ) {

							LinkedPositions.Node next = node.getNext();
							if( next == null )
								break;

							int yID	  =	next.getId();
							if( buffer.contains(yID) ){
								node = next;
								continue;
							}

							if( A[yID] == Integer.MIN_VALUE ){
								node = next;
								continue;
							}

							Item y = dataSet[yID];
							int yPos  =	next.getPosition();
							int ySize =	y.size();

							// Jaccard constraint : another condition:"xSize < ySize * threshold" is not satisfied due to increasing ordering for dataSet.
							if( ySize < xSize * threshold ){
								next.remove();
								continue;
							}

							alpha[yID] = (int)Math.ceil( coff * ( ySize + xSize ) );
							A[yID]++;

							// arugumnet taht global oerdered x and y has same sequence after *Pos
							// ubound don't needs '+1' because of xPos is pointer from id=0 ;
							int ubound = Math.min( xSize - xPos - 1, ySize - yPos -1 );
							if( A[yID] + ubound < alpha[yID] )
								A[yID] = Integer.MIN_VALUE;
							else{
								// execute in only first phase!
								if( A[yID] == 1 ){
									// Hamming Distance Constraint : Hamming distance between part of x and y  after *Pos must exceed hmax.
									// h' <= hmax + ( xPos + 1 + yPos +1 ) - 2 = |x| + |y| - 2α = |x| + |y| - 2t / ( 1 + t )
									int hmax = xSize + ySize - 2 * (int)Math.ceil( coff * ( ySize + xSize ) - (xPos + yPos) ); // ubound don't needs '+2' because of xPos is pointer from id=0 ;
									int h = suffixFilter( x.getTokens(), xPos+1, xSize-xPos-1, y.getTokens(), yPos+1, ySize-yPos-1, hmax, 0 );
									if( hmax < h )
										A[yID] = Integer.MIN_VALUE;
								}
							}
							node = next;

						}

					}
				}

			else{

				for( int xPos = 0 ; xPos < maxPrefixLength ; xPos++ ){

					String w = x.get(xPos);
					LinkedPositions positions = index.get(w);
					if( positions != null ){

						LinkedPositions.Node node = positions.getRootNode();
						while( true ) { // positionSet don't have components with the same position.id

							LinkedPositions.Node next = node.getNext();
							if( next == null )
								break;

							int yID   = next.getId();
							if( buffer.contains(yID) ){
								next.remove();
								continue;
							}

							if( A[yID] == Integer.MIN_VALUE ){
								node = next;
								continue;
							}

							Item y	  = dataSet[yID];
							int yPos  = next.getPosition();
							int ySize = y.size();

							// Jaccard constraint , and another constraint( xSize < ySize * threshold ) is already satisfied.
							if( ySize < xSize * threshold ){
								node = next;
								continue;
							}

							alpha[yID]	= (int)Math.ceil( coff * ( ySize + xSize ) );
							A[yID]++;
							int ubound = Math.min( xSize - xPos -1 , ySize - yPos -1 );
							if( A[yID] + ubound < alpha[yID] )
								A[yID] = Integer.MIN_VALUE;

							node = next;
						}
					}
				}

			}
			List<Item> S = new ArrayList<Item>();
			veryfy( xDataSetID, dataSet, maxPrefixLength, A, prefixLengths, alpha, S, buffer );
			if( 0 < S.size() ){
				buffer.add( xDataSetID );
				S.add(x);
				boolean isUnioned = union( S, result, threshold, buffer );
				if( !isUnioned )
					result.add(S);
			}
			else{
				S.add(x);
				boolean isUnioned = union( S, result, threshold, buffer );
				if(!isUnioned){
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

	private void veryfy( int xDataSetID, Item[] dataSet, int maxXPrefixLength, int[] A, int[] prefixLengths, int[] alpha, List<Item> S, Set<Integer> buffer ){

		Item x = dataSet[xDataSetID];
		String wx_lastPrefix = x.get( maxXPrefixLength-1 );
		for( int yDataSetID = 0 ; yDataSetID < xDataSetID ; yDataSetID++ ){

			if( A[yDataSetID] <= 0 )
				continue;

			int overlapValue = A[yDataSetID];
			Item y = dataSet[yDataSetID];
			String wy_lastPrefix = y.get( prefixLengths[yDataSetID]-1 );
			if( wx_lastPrefix.compareTo( wy_lastPrefix ) < 0 ){ // wx < wy
				int unbound = A[yDataSetID] + x.size() - maxXPrefixLength;
				if( alpha[yDataSetID] <= unbound )
					overlapValue += overlap.calcByMerge( x.getTokens(), maxXPrefixLength, y.getTokens(), A[yDataSetID] );
			}
			else{
				int unbound = A[yDataSetID] + y.size() - prefixLengths[yDataSetID];
				if( alpha[yDataSetID] <= unbound )
					overlapValue += overlap.calcByMerge( x.getTokens(), A[yDataSetID], y.getTokens(), prefixLengths[yDataSetID] );
			}

			if( alpha[yDataSetID] <= overlapValue ){
				S.add(y);
				buffer.add( yDataSetID );
			}
		}

	}

	class Partition{
		int slPos;
		int slLen;
		int srPos;
		int srLen;
		int isRange;
		int notFind;
	}

}