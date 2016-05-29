/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course;

import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.providers.FileIOMoviesProvider;
import univ.bigdata.course.providers.MoviesProvider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class MoviesReviewsQueryRunner {

    public static void main(String[] args) {

    	PrintStream outFile = null;

//    	if (args.length != 2) {
//        	System.out.println("invalid arguments" + args);
//        	return;
//        }
		String inputFile;
		String outputFile;  
    	String file0 = args[0];
    	String file1 = args[1];
    	if(file0.contains("inputFile")){
    		inputFile =  args[0].split("=")[1]; //comment
    		outputFile = args[1].split("=")[1]; //comment
    	}else{
    		inputFile =  args[1].split("=")[1]; //comment
    		outputFile = args[0].split("=")[1]; //comment
    	}
		try {
			outFile = new PrintStream (new File(outputFile)); //comment
//		outFile = new PrintStream (new File("queries_out.txt")); // not comment
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        	
        final PrintStream printer = outFile;
        try{
            final MoviesProvider provider = new FileIOMoviesProvider(inputFile);//comment
//    	final MoviesProvider provider = new FileIOMoviesProvider("movies-sample.txt");// not comment
            final IMoviesStorage storage = new MoviesStorage(provider);

            printer.println("Getting list of total movies average.");
            // 1.
            printer.println();
            printer.println("TOP2.");
            storage.getTopKMoviesAverage(2).stream().forEach(printer::println);
            printer.println();
            printer.println("TOP4.");
            storage.getTopKMoviesAverage(4).stream().forEach(printer::println);

            // 2.
            printer.println("Total average: " + storage.totalMoviesAverageScore());

            // 3.
            printer.println();
            printer.println("The movie with highest average:  " + storage.movieWithHighestAverage());

            // 4.
            printer.println();
            storage.reviewCountPerMovieTopKMovies(4)
                    .entrySet()
                    .stream()
                    .forEach(pair -> printer.println("Movie product id = [" + pair.getKey() + "], reviews count [" + pair.getValue() + "]."));

            // 5.
            printer.println();
            printer.println("The most reviewed movie product id is " + storage.mostReviewedProduct());

            // 6.
            printer.println();
            printer.println("Computing 90th percentile of all movies average.");
            storage.getMoviesPercentile(90).stream().forEach(printer::println);

            printer.println();
            printer.println("Computing 50th percentile of all movies average.");
            storage.getMoviesPercentile(50).stream().forEach(printer::println);

            // 7.
            printer.println();
            printer.println("Computing TOP100 words count");
            storage.moviesReviewWordsCount(100)
                    .entrySet()
                    .forEach(pair -> printer.println("Word = [" + pair.getKey() + "], number of occurrences [" + pair.getValue() + "]."));

            // 8.
            printer.println();
            printer.println("Computing TOP100 words count for TOP100 movies");
            storage.topYMoviewsReviewTopXWordsCount(100, 100)
                    .entrySet()
                    .forEach(pair -> printer.println("Word = [" + pair.getKey() + "], number of occurrences [" + pair.getValue() + "]."));

            printer.println("Computing TOP100 words count for TOP10 movies");
            storage.topYMoviewsReviewTopXWordsCount(100, 10)
                    .entrySet()
                    .forEach(pair -> printer.println("Word = [" + pair.getKey() + "], number of occurrences [" + pair.getValue() + "]."));

            // 9.
            printer.println();
            printer.println("Most popular movie with highest average score, reviewed by at least 20 users " + storage.mostPopularMovieReviewedByKUsers(20));
            printer.println("Most popular movie with highest average score, reviewed by at least 15 users " + storage.mostPopularMovieReviewedByKUsers(15));
            printer.println("Most popular movie with highest average score, reviewed by at least 10 users " + storage.mostPopularMovieReviewedByKUsers(10));
            printer.println("Most popular movie with highest average score, reviewed by at least 5 users " + storage.mostPopularMovieReviewedByKUsers(5));

            // 10.
            printer.println();
            printer.println("Compute top 10 most helpful users.");
            storage.topKHelpfullUsers(10)
                    .entrySet()
                    .forEach(pair -> printer.println("User id = [" + pair.getKey() + "], helpfulness [" + pair.getValue() + "]."));

            printer.println();
            printer.println("Compute top 100 most helpful users.");
            storage.topKHelpfullUsers(100)
                    .entrySet()
                    .forEach(pair -> printer.println("User id = [" + pair.getKey() + "], helpfulness [" + pair.getValue() + "]."));

            // 11.
            printer.println();
            printer.println("Total number of distinct movies reviewed [" +storage.moviesCount() + "].");
            printer.println("THE END.");
        } catch (final Exception e) {
            e.printStackTrace();
        }

    }
}
