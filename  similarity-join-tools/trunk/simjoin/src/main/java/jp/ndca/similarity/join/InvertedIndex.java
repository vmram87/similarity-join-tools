package jp.ndca.similarity.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class InvertedIndexReadOnly{

	// word, Position
	Map<String, PositionLists> positionsMap = new HashMap<String, PositionLists>();

	public PositionLists get( String str ){
		return positionsMap.get(str);
	}

	public void put( String str, int id, int pointer ){
		PositionLists positions = positionsMap.get(str);
		if( positions == null ){
			positions = new PositionLists();
			positionsMap.put(str, positions);
		}
		positions.put( id, pointer );
	}

	public int size(){
		return positionsMap.size();
	}

	public Set<String> keySet(){
		return positionsMap.keySet();
	}

}

/**
 * this class is for "InvertedIndexReadOnly" and this is Thread unsafe class.
 *
 * @author hattori_tsukasa
 *
 */
class PositionLists{

	List<Integer> idList;
	List<Integer> pointerList;

	public PositionLists(){
		this.idList = new ArrayList<Integer>();
		this.pointerList = new ArrayList<Integer>();
	}

	public void put ( int id , int pointer ){
		this.idList.add(id);
		this.pointerList.add(pointer);
	}

	public int size(){
		return idList.size();
	}

}


class InvertedIndex_Removable{

	int dataSize;

	// word, Position
	Map<String, Positions> positionsMap = new HashMap<String, Positions>();

	/**
	 * constractor
	 * @param size
	 */
	public InvertedIndex_Removable(int size){
		this.dataSize = size;
	}


	/**
	 * get str's positions
	 * @param str
	 * @return
	 */
	public Positions get( String str ){
		return positionsMap.get(str);
	}


	/**
	 * put id and position into str's Inverted-Index.
	 * @param str
	 * @param id
	 * @param pointer
	 */
	public void put( String str, int id, int pointer ){
		Positions positions = positionsMap.get(str);
		if( positions == null ){
			positions = new Positions(dataSize);
			positionsMap.put(str, positions);
		}
		positions.put( id, pointer );
	}


	/**
	 * get number of kinds of word.
	 * @return
	 */
	public int size(){
		return positionsMap.size();
	}

	/**
	 * get word's set
	 * @return
	 */
	public Set<String> keySet(){
		return positionsMap.keySet();
	}

}


/**
 * this class is for "InvertedIndexRemovable" and this is Thread unsafe class.
 *
 * @author hattori_tsukasa
 *
 */
class Positions {

	int[] positions;

	Set<Integer> stockIDs = new HashSet<Integer>();

	public Positions( int size ){
		positions = new int[size];
	}

	public void put( int id, int pointer ){
		positions[id] = pointer;
		stockIDs.add(id);
	}

	public void remove( int id ){
		positions[id] = 0;
		stockIDs.remove(id);
	}

	public int get(int id){
		return positions[id];
	}

}


class InvertedIndexRemovable{

	int dataSize;

	// word, Position
	Map<String, Positions> positionsMap = new HashMap<String, Positions>();

	/**
	 * constractor
	 * @param size
	 */
	public InvertedIndexRemovable(int size){
		this.dataSize = size;
	}


	/**
	 * get str's positions
	 * @param str
	 * @return
	 */
	public Positions get( String str ){
		return positionsMap.get(str);
	}


	/**
	 * put id and position into str's Inverted-Index.
	 * @param str
	 * @param id
	 * @param pointer
	 */
	public void put( String str, int id, int pointer ){
		Positions positions = positionsMap.get(str);
		if( positions == null ){
			positions = new Positions(dataSize);
			positionsMap.put(str, positions);
		}
		positions.put( id, pointer );
	}


	/**
	 * get number of kinds of word.
	 * @return
	 */
	public int size(){
		return positionsMap.size();
	}

	/**
	 * get word's set
	 * @return
	 */
	public Set<String> keySet(){
		return positionsMap.keySet();
	}


}
