package Recommender;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CoFactorMapper extends Mapper<LongWritable, Text, Text, Text> {

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\t");//["2","1=0.47368421052631576"]
			context.write(new Text(line[0]), new Text(line[1]));//"1=0.47368421052631576"
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user,movie,rating
			//pass data to reducer

			String[] line = value.toString().split(",");//["2","1","3"]
			context.write(new Text(line[1]), new Text(line[0] + ':' + line[2]));//key =>1 value => 2:3 //2:3

		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			
			Map<String, Double> coOccurenceMap = new HashMap<String, Double>();
			Map<String, Double> userPreferenceMap = new HashMap<String, Double>();
		
			for (Text value:values){
				
				
				if (value.toString().contains("=")){
					String[] movie_relation = value.toString().split("=");//[1,0.47368421052631576]
					coOccurenceMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				}
				else{
					String[] user_rating = value.toString().split(":");
					userPreferenceMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}
			
			for(Map.Entry<String, Double> entry: coOccurenceMap.entrySet()){
				String movie = entry.getKey();
				double preference = entry.getValue();

				for (Map.Entry<String, Double> element: userPreferenceMap.entrySet()){
					String user = element.getKey();
					double rating = element.getValue();
					context.write(new Text(user + ":" + movie), new DoubleWritable(preference*rating));
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CoFactorMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CoFactorMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoFactorMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
