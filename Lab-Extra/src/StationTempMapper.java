import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StationTempMapper extends Mapper<LongWritable, Text, StationTempKey, Text> {
	private StationTempKey outKey = new StationTempKey();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if(line.length() >= 92){

            String station = line.substring(4,10) + "-" + line.substring(10, 15);
            String year = line.substring(15,19);

            int temp = Integer.parseInt(line.substring(87,92).trim());

            outKey.set(station, temp);
            outValue.set(year);

            context.write(outKey, outValue);
        }
    }
}
