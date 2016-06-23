package comparators;

import java.util.Comparator;

import scala.Tuple2;

public class PageRankComperator implements Comparator<Tuple2<String, Double>>{

	@Override
	public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
		if(o1._2 == o2._2){
			return o1._1.compareTo(o2._1);
		}else{
			return o1._2.compareTo(o2._2);
		}
	}

}
