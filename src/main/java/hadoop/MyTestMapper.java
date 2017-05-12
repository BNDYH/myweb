package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyTestMapper {
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			final Counter counter = context.getCounter("word", "hello");
			final Counter lineContext = context.getCounter("word","line");
			lineContext.increment(1L);
			if(value.toString().contains("hello"))
			{
				counter.increment(1L);
			}
			System.out.println("********map运行************");
			String[] words = value.toString().split("\\s");
			for (String word : words) {
				System.out.println(word);
				context.write(new Text(word), new LongWritable(1));
			}
		}
		
	}
	public static class WordCountReducer extends Reducer<Text, LongWritable,Text, LongWritable> {
		@Override
		protected void reduce(Text word, Iterable<LongWritable> times,
				Reducer<Text,LongWritable,  Text, LongWritable>.Context context) throws IOException, InterruptedException {
			Long sum=0L;
			// TODO Auto-generated method stub
			System.out.println("********reduce运行************");
			for (LongWritable time : times) {
				sum+=time.get();
			}
			context.write(word, new LongWritable(sum));
		}
		
	}
	public static class MyPartitioner extends Partitioner<Text, LongWritable>
	{

		@Override
		public int getPartition(Text key, LongWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			if("hello".equals(key.toString()))
			{
				return 0;				
			}else
			{
				return 1;
			}
		}
		
	}
	public static void main(String[] args) throws Exception {
		final String INPUT_PATH = "hdfs://192.168.4.185:9000/bndyh/input/hello";
		final String OUTPUT_PATH = "hdfs://192.168.4.185:9000/bndyh/output";
		final Configuration conf = new Configuration();
		final Job job=new Job(conf);
		System.out.println("********设置job************");
		job.setJobName(MyTestMapper.class.getSimpleName());
		job.setJarByClass(MyTestMapper.class);
		
		job.setNumReduceTasks(1);
		job.setPartitionerClass(MyPartitioner.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		System.out.println("********job设置完成************");
		job.waitForCompletion(true);
		System.out.println("********job提交************");
	}
}
