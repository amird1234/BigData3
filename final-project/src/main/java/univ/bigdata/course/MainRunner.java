package univ.bigdata.course;

public class MainRunner {

    public static void main(String[] args) {
    	long moviesNum = 0;
    		
    	String inputPath = args[0];

    	//initialize movie storage and spark context
    	MoviesStorage moviesStorage = new MoviesStorage(inputPath);
        
    	//call first implemented function
        moviesNum = moviesStorage.moviesCount();
        System.out.println("number of movies " + moviesNum);
    }
}
