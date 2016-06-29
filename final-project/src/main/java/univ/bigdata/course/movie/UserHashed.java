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
