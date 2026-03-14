
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StationTemp extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Station Temp");

		job.setJarByClass(StationTemp.class);

		job.setMapperClass(StationTempMapper.class);
		job.setReducerClass(StationTempReducer.class);

		job.setMapOutputKeyClass(StationTempKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		    System.err.println("Usage: StationTemp <input path> <output path>");
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
		
		int res = ToolRunner.run(conf, new StationTemp(), args);
		System.exit(res);
	}
	
}
