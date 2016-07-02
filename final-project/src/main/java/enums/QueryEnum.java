/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package enums;

public enum QueryEnum {
	TOP_K_MOVIE_AVE ("getTopKMoviesAverage",0),
	TOT_MOV_AVE_SCO("totalMoviesAverageScore",1);
	
	private String stringValue;
	private Integer intValue;
	QueryEnum(String name, Integer value){
		stringValue = name;
		intValue = value;
	}

}
