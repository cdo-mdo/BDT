import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearDescComparator extends WritableComparator {

    protected YearDescComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text year1 = (Text) a;
        Text year2 = (Text) b;

        int y1 = Integer.parseInt(year1.toString());
        int y2 = Integer.parseInt(year2.toString());

        return Integer.compare(y2, y1);  // reverse order
    }
}