package Recommender;

import Recommender.CoFactorGenerator;
import Recommender.MoviesRatingByUser;
import Recommender.Multiplication;
import Recommender.Normalize;
import Recommender.Accumulate;


public class Driver {
	public static void main(String[] args) throws Exception {
		

		HiveJDBCClient movieRatingByUser = new HiveJDBCClient();
		CoFactorGenerator coFactorGenerator = new CoFactorGenerator();
		Normalize normalize = new Normalize();	
		Multiplication multiplication = new Multiplication();
		Accumulate accumulate = new Accumulate();


		
		//"/home/cloudera/workspace/cs626_proj_v2/input/mv_0000001.txt"
		String appDir = "/home/cloudera/workspace/cs626_proj_v2";
		String RAW_INPUT = "/input/preprocessed_1.txt";
//		String appDir = "/proj";
//		String RAW_INPUT = "/input";
		String moviesRatingByuserOutput = "/movieratingbyuser/part-r-00000.txt";
		String coFactor = "/cofactor";
		String normaliseOuput = "/normalize";
		String multiplyOutput = "/multiply";
		String accumulateOutput = "/accumulate";
		String OUTPUT_NAME = "/part-r-00000";
		

		String[] moviesRatingByUserArgs = {"/home/cloudera/workspace/cs626_proj_v2/input/.txt","./movieratingbyuser/part-r-00000.txt"};
		String[] coFactorArgs = {appDir+moviesRatingByuserOutput,appDir+coFactor};
		
		String[] normalizeArgs = {appDir+coFactor+OUTPUT_NAME,appDir+normaliseOuput};
		
		String[] multiplyArgs = {appDir+normaliseOuput, appDir+RAW_INPUT, appDir+multiplyOutput };
		String[] accumulateArgs = {appDir+multiplyOutput,appDir+accumulateOutput};
		
		System.out.println("Staring Hadoop Recommender System");
		movieRatingByUser.main(moviesRatingByUserArgs);
		coFactorGenerator.main(coFactorArgs);
		normalize.main(normalizeArgs);
		multiplication.main(multiplyArgs);
		accumulate.main(accumulateArgs);
		System.out.println("Recommender System Execution Completed");

	}

}
