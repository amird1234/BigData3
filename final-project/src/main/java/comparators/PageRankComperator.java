package comparators;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;
import univ.bigdata.course.movie.PageRankResults;

public class PageRankComperator implements Comparator<PageRankResults>, Serializable{

	@Override
	public int compare(PageRankResults o1, PageRankResults o2) {
		if(o1.score == o2.score){
			return o2.id.compareTo(o1.id);
		}else{
			return o2.score.compareTo(o1.score);
		}
	}

}
