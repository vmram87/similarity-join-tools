package jp.ndca.similarity.join;

import java.util.List;
import java.util.Set;
import java.util.Map.Entry;


public interface SimilarityJoin {

	public Item convert( String dataset, int id );
	public Item[] convert( List<String> dataset );

	public List<Item> search( Item query, Item[] dataSet, double threshold );

	public List<Entry<Item,Item>> extractPairs( Item[] dataSet, double threshold );

	public List<Set<Item>> extractBulks( Item[] dataSet, double threshold );

}