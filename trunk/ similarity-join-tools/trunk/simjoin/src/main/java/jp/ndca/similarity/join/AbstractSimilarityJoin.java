package jp.ndca.similarity.join;

import java.util.List;
import java.util.Set;

import jp.ndca.similarity.distance.Jaccard;

public abstract class AbstractSimilarityJoin implements SimilarityJoin{

	protected final static Jaccard jaccard = new Jaccard();

	protected boolean union( List<Item> S, List<List<Item>> result, double threshold, Set<Integer> buffer ){

		boolean isUnioned = false;
		String[] query = S.get(0).getTokens();
		int querySize = query.length;

		for( List<Item> set : result ){
			String[] candidate = set.get(0).getTokens();
			int candidateSize = candidate.length;

			// Jaccard Constraint
			if( querySize < threshold * candidateSize || candidateSize < threshold * querySize )
				continue;

			double score = jaccard.calcByMerge(query, candidate);
			if( threshold <= score ){
				set.addAll(S);
				isUnioned = true;
				break;
			}
		}
		return isUnioned;

	}

}
