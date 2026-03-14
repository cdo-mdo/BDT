import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StationTempKey implements WritableComparable<StationTempKey> {

    private Text stationId;
    private IntWritable temperature;

    public StationTempKey() {
        stationId = new Text();
        temperature = new IntWritable();
    }

    public void set(String station, int temp){
        stationId.set(station);
        temperature.set(temp);
    }

    public Text getStationId(){ return stationId; }
    public IntWritable getTemperature(){ return temperature; }

    @Override
    public int compareTo(StationTempKey other){

        int cmp = stationId.compareTo(other.stationId);

        if(cmp != 0) {
            return cmp;
        }

        // descending temperature
        return -1 * temperature.compareTo(other.temperature);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        stationId.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationId.readFields(in);
        temperature.readFields(in);
    }

}