import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AveTemp1 extends Configured implements Tool {

	public static class AveTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text outYear = new Text();
		private IntWritable outTemp = new IntWritable();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
	        if (line.length() >= 92) {
	        	String year = line.substring(15, 19).trim();
	            String tempStr = line.substring(87, 92).trim();

	            try {
	                int temperature = Integer.parseInt(tempStr)/10;

	                outYear.set(year);
	                outTemp.set(temperature);
	                context.write(outYear, outTemp);

	            } catch (NumberFormatException e) {
	                // Ignore invalid temperature
	            }
	        }
			
		}
		
	}
	
	public static class AveTempReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			
			for (IntWritable val : values) {
	            sum += val.get();
	            count++;
	        }

			if (count > 0) {
	            double average = (double) sum / count;
	            result.set(average);
	            context.write(key, result);
	        }
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		    System.err.println("Usage: AveTemp1 <input path> <output path>");
		    System.exit(-1);
		}

		Configuration conf = new Configuration();

		Path output = new Path(args[1]);
		try (FileSystem fs = output.getFileSystem(conf)) {
            if (fs.exists(output)) {
                // recursive = true deletes non-empty directories
                boolean deleted = fs.delete(output, true);
                System.out.println("Deleted " + output + ": " + deleted);
            } else {
                System.out.println("Path does not exist: " + output);
            }
        }
		
		int res = ToolRunner.run(conf, new AveTemp1(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Average Temperature 1");
		job.setJarByClass(AveTemp1.class);
		
		job.setMapperClass(AveTempMapper.class);
		job.setReducerClass(AveTempReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Run job with 2 reducers
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
	}

}
