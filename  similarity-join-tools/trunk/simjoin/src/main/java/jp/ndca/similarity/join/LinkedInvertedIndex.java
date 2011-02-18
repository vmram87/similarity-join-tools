package jp.ndca.similarity.join;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * InvertedIndex For SimilarityJoin algorithm
 * @author hattori_tsukasa
 *
 */
public class LinkedInvertedIndex{

	// word, Position
	Map<String, LinkedPositions> positionsMap = new HashMap<String, LinkedPositions>();


	/**
	 * get str's positions
	 * @param str
	 * @return
	 */
	public LinkedPositions get( String str ){
		return positionsMap.get(str);
	}


	/**
	 * put id and position into str's Inverted-Index.
	 * @param str
	 * @param id
	 * @param pointer
	 */
	public void put( String str, int id, int pointer ){
		LinkedPositions positions = positionsMap.get(str);
		if( positions == null ){
			positions = new LinkedPositions();
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

class LinkedPositions {

	private Node root;

	private Node last;

	int size;

	public LinkedPositions(){
		Node node = new Node();
		root = node;
		last = node;
	}

	class Node{
		private int position;
		private int id;
		private Node next;
		private Node pre;
		public int getPosition() {
			return position;
		}
		public void setPosition(int position) {
			this.position = position;
		}
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public Node getNext() {
			return next;
		}
		public void setNext(Node next) {
			this.next = next;
		}
		public Node getPre() {
			return pre;
		}
		public void setPre(Node pre) {
			this.pre = pre;
		}

		public void remove(){
			if( pre != null )
				pre.next = next;
			if( next != null )
				next.pre = pre;
			else // if( next == null ) → this node is last node.
				last = pre;
			size--;
		}
	}

	public void put( int id, int pointer ){
		Node node = new Node();
		node.setId(id);
		node.setPosition(pointer);
		node.setPre(last);
		last.next = node;
		last = node;
		size++;
	}

	public Node getRootNode(){
		return root;
	}

}

