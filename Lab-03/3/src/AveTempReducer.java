import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveTempReducer extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<SumCountWritable> values, Context context)
            throws IOException, InterruptedException {

        double totalSum = 0.0;
        int totalCount = 0;

        for (SumCountWritable val : values) {
            totalSum += val.getSum();
            totalCount += val.getCount();
        }

        if (totalCount > 0) {
            double average = totalSum / totalCount;
            result.set(average);
            context.write(key, result);
        }
    }
}