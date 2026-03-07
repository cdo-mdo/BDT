import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveTempReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0.0;
		int count = 0;
		
		for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

		if (count > 0) {
			double average = sum / count;
            result.set(average);
            context.write(key, result);
        }
	}


}
