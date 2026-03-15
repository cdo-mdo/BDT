import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroGenericStationTempYear extends Configured implements Tool {

    public static class AvroMapper
            extends Mapper<LongWritable, Text, IntWritable, StationTempWritable> {

        private final NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
        private final IntWritable outYear = new IntWritable();
        private final StationTempWritable outValue = new StationTempWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            utils.parse(value.toString());

            if (utils.isValidTemperature()) {
                outYear.set(utils.getYearInt());
                outValue.set(utils.getStationId(), utils.getAirTemperature());
                context.write(outYear, outValue);
            }
        }
    }

    public static class AvroReducer
            extends Reducer<IntWritable, StationTempWritable, AvroKey<GenericRecord>, NullWritable> {

        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = AvroJob.getOutputKeySchema(context.getConfiguration());
        }

        @Override
        protected void reduce(IntWritable key, Iterable<StationTempWritable> values, Context context)
                throws IOException, InterruptedException {

            float maxTemp = Float.NEGATIVE_INFINITY;
            String maxStationId = "";

            for (StationTempWritable value : values) {
                float temp = value.getTemperature();
                if (temp > maxTemp) {
                    maxTemp = temp;
                    maxStationId = value.getStationId();
                }
            }

            GenericRecord record = new GenericData.Record(schema);
            record.put("year", key.get());
            record.put("maxTemp", maxTemp);
            record.put("stationId", maxStationId);

            context.write(new AvroKey<>(record), NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s <input> <output> <schema-file>%n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AvroGenericStationTempYear.class);
        job.setJobName("Avro Year Max Temp With Station");

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outputPath);

        Schema schema = new Schema.Parser().parse(new File(args[2]));

        job.setMapperClass(AvroMapper.class);
        job.setReducerClass(AvroReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(StationTempWritable.class);

        AvroJob.setOutputKeySchema(job, schema);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new AvroGenericStationTempYear(), args);
        System.exit(res);
    }
}