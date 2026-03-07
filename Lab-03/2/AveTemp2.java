import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AveTemp2 extends Configured implements Tool {

	public static class AveTempMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {
	    private Text outYear = new Text();
	    private SumCountWritable outValue = new SumCountWritable();

	    @Override
	    public void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	        String line = value.toString();

	        if (line.length() >= 92) {
	            String year = line.substring(15, 19).trim();
	            String tempStr = line.substring(87, 92).trim();

	            try {
	                int rawTemp = Integer.parseInt(tempStr);

	                if (rawTemp != 9999) {
	                    double temperature = rawTemp / 10.0;
	                    outYear.set(year);
	                    outValue.set(temperature, 1);
	                    context.write(outYear, outValue);
	                } 
	            } catch (NumberFormatException e) {
	                // Ignore invalid temperature
	            }
	        }
	    }
	}

	public static class AveTempCombiner extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {
	    private SumCountWritable result = new SumCountWritable();

	    @Override
	    public void reduce(Text key, Iterable<SumCountWritable> values, Context context)
	            throws IOException, InterruptedException {
	        double sum = 0.0;
	        int count = 0;

	        for (SumCountWritable val : values) {
	            sum += val.getSum();
	            count += val.getCount();
	        }

	        result.set(sum, count);
	        context.write(key, result);
	    }
	}
	
	public static class AveTempReducer extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {
	    private DoubleWritable result = new DoubleWritable();

	    @Override
	    public void reduce(Text key, Iterable<SumCountWritable> values, Context context)
	            throws IOException, InterruptedException {
	        double sum = 0.0;
	        int count = 0;

	        for (SumCountWritable val : values) {
	            sum += val.getSum();
	            count += val.getCount();
	        }

	        if (count > 0) {
	            result.set(sum / count);
	            context.write(key, result);
	        }
	    }
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		    System.err.println("Usage: AveTemp2 <input path> <output path>");
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
		
		int res = ToolRunner.run(conf, new AveTemp2(), args);
		System.exit(res);

	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Average Temperature 2");
		job.setJarByClass(AveTemp2.class);
		
		job.setMapperClass(AveTempMapper.class);
		job.setCombinerClass(AveTempCombiner.class);
		job.setReducerClass(AveTempReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SumCountWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Run job with 1 reducers
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
	}

}
