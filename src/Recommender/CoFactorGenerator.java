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

public class CoFactorGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			
			String[] userMovieRatings = line.split("\t");//[1	, "2:5,1:3"]
			String[] movieRatings = userMovieRatings[1].split(","); // [2:5,1:3]
			// 1	2:5,1:3
			
			// movie1: rating, movie2: rating...
			
			for(int i = 0; i < movieRatings.length; i++){//2
				String movie1 = movieRatings[i].trim().split(":")[0];//[2:5]=>[2,5]=>[2]

				for(int j = 0; j < movieRatings.length; j++){
					String movie2 = movieRatings[j].trim().split(":")[0];//[2:5]=>[2,5]=>[2] //[1:3]=>[1,3]=>1
					// movie1 = 2 and movie2 = 2
					// movie1 = 2 and movie2 = 1
					// movie1 = 1 and movie2 = 2
					// movie1 = 1 and movie2 = 1
					
					context.write(new Text(movie1 + ':' + movie2), new IntWritable(1));
				}
			}
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		
			int sum = 0;
			while(values.iterator().hasNext()){
				sum += values.iterator().next().get();
			}
			
			context.write(key, new IntWritable(sum));
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoFactorGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
