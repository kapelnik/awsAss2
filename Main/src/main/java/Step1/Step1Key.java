import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class Step1Key implements WritableComparable<Step1Key> {
    //key = {w1, w2, decade}

    private String kohavit ="*";
    private Text w1;
    private Text w2;
    private Text decade;
    public Step1Key(){
        this.w1=new Text();
        this.w2=new Text();
        this.decade=new Text();

    }
    public Step1Key(Text w1, Text w2, Text decade) {
        super();
        this.w1 = w1;
        this.w2 = w2;
        this.decade = decade;
    }
    public Text getW1() {
        return w1;
    }
    public Text getW2() {
        return w2;
    }
    public Text getDecade() { return decade; }

    public boolean isFirst() {
        return w2.toString().equals(kohavit)&&!w1.toString().equals(kohavit);
    }
    public boolean isSecond() {
        return w1.toString().equals(kohavit)&&!w2.toString().equals(kohavit);
    }
    public boolean isN() {
        return w1.toString().equals(kohavit) && w2.toString().equals(kohavit);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.w1.write(out);
        this.w2.write(out);
        this.decade.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.w1.readFields(in);
        this.w2.readFields(in);
        this.decade.readFields(in);
    }

    @Override
    public int compareTo(Step1Key other) { // N > decade > w1 > w2
        if (isN() && other.isN())
            return 0;
        if (this.isN())// im * *
            return -1;
        if(other.isN())// he is  * *
            return 1;

        if (decade.compareTo(other.decade) > 0)
            return 1;
        if (decade.compareTo(other.decade) < 0)
            return -1;

        if (w1.compareTo(other.w1) > 0)
            return 1;
        if (w1.compareTo(other.w1) < 0)
            return -1;

        return w2.compareTo(other.w2);
    }
    @Override
    public String toString() {
        return w1 + " " + w2 + " " + decade;
    }

}
