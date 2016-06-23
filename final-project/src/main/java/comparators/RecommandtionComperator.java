package comparators;

import java.util.Comparator;

import scala.Tuple2;

public class RecommandtionComperator implements Comparator<Tuple2<Double, String>>{

	@Override
	public int compare(Tuple2<Double, String> o1, Tuple2<Double, String> o2) {
		return o1._2.compareTo(o2._2);
	}

}
