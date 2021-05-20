package Recommender;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MoviesRatingByUser {
	public static class MoviesRatingByUserMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// MAP_Method:divide data by user

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input user,movie,rating
			String[] userMovieRating = value.toString().trim().split(","); // we have 3 list here 
			int userId = Integer.parseInt(userMovieRating[0]);
			String movieId = userMovieRating[1];
			String rating = userMovieRating[2];
		

			context.write(new IntWritable(userId), new Text(movieId + ':' + rating));

		}
	}

	public static class MoviesRatingByUserReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		// reduce method

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//merge data for one user
			StringBuilder moviesRatingList = new StringBuilder();
			
			while (values.iterator().hasNext()){
				moviesRatingList.append("," + values.iterator().next());
			}
			
			// key-value pair: key = userID value = movie1: rating_score, movie2: rating_score.....
			context.write(key, new Text(moviesRatingList.toString().replaceFirst(",", "")));

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(MoviesRatingByUserMapper.class);
		job.setReducerClass(MoviesRatingByUserReducer.class);

		job.setJarByClass(MoviesRatingByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
        
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		 // select userid,concat_ws(",",collect_list(concat_ws(':',cast(movieid as string),cast(rating as string)))) as movierating from netflixtable group by userid;
	}

}
