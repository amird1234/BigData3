/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
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
