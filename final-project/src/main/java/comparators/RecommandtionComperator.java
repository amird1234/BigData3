package comparators;

import java.util.Comparator;

import scala.Tuple2;
import scala.Serializable;

public class RecommandtionComperator implements Comparator<Tuple2<Double, String>>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<Double, String> o1, Tuple2<Double, String> o2) {
		return o2._1.compareTo(o1._1);
	}

}
