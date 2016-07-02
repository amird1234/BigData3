/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
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
