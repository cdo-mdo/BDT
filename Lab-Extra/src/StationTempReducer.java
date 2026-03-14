import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StationTempReducer extends Reducer<StationTempKey, Text, Text, Text> {
	public void reduce(StationTempKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for(Text year : values){

            String output = key.getStationId().toString()
                    + " "
                    + key.getTemperature().get()
                    + " "
                    + year.toString();

            context.write(null, new Text(output));
        }
    }

}
