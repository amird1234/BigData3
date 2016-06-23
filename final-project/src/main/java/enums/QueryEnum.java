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
