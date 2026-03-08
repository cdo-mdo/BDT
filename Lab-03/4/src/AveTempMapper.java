import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AveTempMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {

    private Map<String, SumCountWritable> localMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        localMap = new HashMap<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.length() >= 92) {
            String year = line.substring(15, 19).trim();
            String tempStr = line.substring(87, 92).trim();

            try {
                int temp = Integer.parseInt(tempStr);

                if (temp != 9999) {
                    double temperature = temp / 10.0;

                    if (localMap.containsKey(year)) {
                        SumCountWritable current = localMap.get(year);
                        current.set(current.getSum() + temperature, current.getCount() + 1);
                    } else {
                        localMap.put(year, new SumCountWritable(temperature, 1));
                    }
                }
            } catch (NumberFormatException e) {
                // Ignore invalid temperature
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text outKey = new Text();

        for (Map.Entry<String, SumCountWritable> entry : localMap.entrySet()) {
            outKey.set(entry.getKey());
            context.write(outKey, entry.getValue());
        }
    }
}