/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course.movie;

import java.io.Serializable;

import org.apache.spark.mllib.recommendation.Rating;

public class UserHashed implements Serializable{
	public String stringValuse;
	public Integer intValue;
	public Rating[]  ratingsArray;
	
	public UserHashed(String s, Integer i){
		stringValuse = s;
		intValue = i;
	}
	
	public UserHashed(Integer i, Rating[] a){
		intValue = i;
		ratingsArray = a;
	}
}
