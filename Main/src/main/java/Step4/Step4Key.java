import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Step4Key implements WritableComparable<Step4Key> {
    //key = {w1, w2,decade,c12,N}

    private String kohavit ="*";
    private Text w1;
    private Text w2;
    private Text decade;
    private Text npmi;
//    private Text N;

    public Step4Key(){
        this.w1=new Text();
        this.w2=new Text();
        this.decade=new Text();
        this.npmi=new Text();
//        this.N=new Text();
    }
    public Step4Key(Text w1, Text w2, Text decade, Text npmi) { //, Text N
        super();
        this.w1 = w1;
        this.w2 = w2;
        this.decade = decade;
        this.npmi = npmi;
//        this.N = N;
    }
    public Text getW1() {
        return this.w1;
    }
    public Text getW2() {
        return this.w2;
    }
    public Text getDecade() { return this.decade; }
    public Text getNpmi() { return this.npmi; }
//    public Text getN() { return this.N; }

    public boolean isFirst() {
        return (this.w2.toString().equals(kohavit))&&(!this.w1.toString().equals(kohavit));
    }
    public boolean isSecond() {
        return (this.w1.toString().equals(kohavit))&&(!this.w2.toString().equals(kohavit));
    }


    @Override
    public void write(DataOutput out) throws IOException {
        this.w1.write(out);
        this.w2.write(out);
        this.decade.write(out);
        this.npmi.write(out);
//        this.N.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.w1.readFields(in);
        this.w2.readFields(in);
        this.decade.readFields(in);
        this.npmi.readFields(in);
//        this.N.readFields(in);
    }


    @Override
    public int compareTo(Step4Key other) { //  decade > npmi > w1 > w2
        if (decade.compareTo(other.decade) > 0)
            return 1;
        if (decade.compareTo(other.decade) < 0)
            return -1;

        if (this.npmi.compareTo(other.npmi) > 0)
            return -1;
        if (this.npmi.compareTo(other.npmi) < 0)
            return 1;

        if (w1.compareTo(other.w1) > 0)
            return 1;
        if (w1.compareTo(other.w1) < 0)
            return -1;

        return w2.compareTo(other.w2);
//        double npmi1 = Double.parseDouble(this.npmi.toString());
//        double npmi2 = Double.parseDouble(other.getNpmi().toString());
//        if (npmi1 > npmi2)
//            return 1;
//        if (npmi1 < npmi2)
//            return -1;
    }
    @Override
    public String toString() {
        return this.w1.toString() + " " + this.w2 + " " + this.getDecade() + " " +this.getNpmi();
    }

}
