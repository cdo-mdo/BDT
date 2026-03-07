import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AveTempMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Text outYear = new Text();
	private DoubleWritable outTemp = new DoubleWritable();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
        if (line.length() >= 92) {
        	String year = line.substring(15, 19).trim();
            String tempStr = line.substring(87, 92).trim();

            try {
                double temperature = Integer.parseInt(tempStr) / 10.0;

                outYear.set(year);
                outTemp.set(temperature);
                context.write(outYear, outTemp);

            } catch (NumberFormatException e) {
                // Ignore invalid temperature
            }
        }
		
	}

}
