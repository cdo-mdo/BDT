import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StationTempWritable implements Writable {

    private final Text stationId = new Text();
    private float temperature;

    public StationTempWritable() {
    }

    public StationTempWritable(String stationId, float temperature) {
        this.stationId.set(stationId);
        this.temperature = temperature;
    }

    public void set(String stationId, float temperature) {
        this.stationId.set(stationId);
        this.temperature = temperature;
    }

    public String getStationId() {
        return stationId.toString();
    }

    public float getTemperature() {
        return temperature;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        stationId.write(out);
        out.writeFloat(temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationId.readFields(in);
        temperature = in.readFloat();
    }

    @Override
    public String toString() {
        return stationId.toString() + "\t" + temperature;
    }
}