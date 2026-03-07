import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<Text, SumCountWritable> {

    @Override
    public int getPartition(Text key, SumCountWritable value, int numPartitions) {
        if (numPartitions == 0) {
            return 0;
        }

        int year = Integer.parseInt(key.toString());

        if (year < 1930) {
            return 0;
        } else {
            return 1 % numPartitions;
        }
    }
}