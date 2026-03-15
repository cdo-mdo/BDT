import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

public class NcdcLineReaderUtils {

    private static final int MISSING_TEMPERATURE = 9999;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");

    private String stationId;
    private String observationDateString;
    private String year;
    private String airTemperatureString;
    private int airTemperatureRaw;
    private float airTemperature;
    private boolean airTemperatureMalformed;
    private String quality;

    public void parse(String record) {
        if (record == null || record.length() < 93) {
            resetAsMalformed();
            return;
        }

        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        observationDateString = record.substring(15, 27);
        year = record.substring(15, 19);
        airTemperatureMalformed = false;

        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else if (record.charAt(87) == '-') {
            airTemperatureString = record.substring(87, 92);
        } else {
            resetAsMalformed();
            quality = record.substring(92, 93);
            return;
        }

        try {
            airTemperatureRaw = Integer.parseInt(airTemperatureString);
            airTemperature = airTemperatureRaw / 10.0f;
        } catch (NumberFormatException e) {
            resetAsMalformed();
            quality = record.substring(92, 93);
            return;
        }

        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    private void resetAsMalformed() {
        stationId = "";
        observationDateString = "";
        year = "";
        airTemperatureString = "";
        airTemperatureRaw = MISSING_TEMPERATURE;
        airTemperature = MISSING_TEMPERATURE / 10.0f;
        airTemperatureMalformed = true;
        quality = "";
    }

    public boolean isValidTemperature() {
        return !airTemperatureMalformed
                && airTemperatureRaw != MISSING_TEMPERATURE
                && quality.matches("[01459]");
    }

    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    public boolean isMissingTemperature() {
        return airTemperatureRaw == MISSING_TEMPERATURE;
    }

    public String getStationId() {
        return stationId;
    }

    public Date getObservationDate() {
        try {
            return DATE_FORMAT.parse(observationDateString);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getYear() {
        return year;
    }

    public int getYearInt() {
        return Integer.parseInt(year);
    }

    public float getAirTemperature() {
        return airTemperature;
    }

    public String getAirTemperatureString() {
        return airTemperatureString;
    }

    public String getQuality() {
        return quality;
    }
}