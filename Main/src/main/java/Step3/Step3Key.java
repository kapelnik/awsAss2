import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Step3Key implements WritableComparable<Step3Key> {
    //key = {w1, w2,decade,c12,N}

    private String kohavit ="*";
    private Text w1;
    private Text w2;
    private Text decade;
//    private Text c12;
//    private Text N;

    public Step3Key(){
        this.w1=new Text();
        this.w2=new Text();
        this.decade=new Text();
//        this.c12=new Text();
//        this.N=new Text();
    }
    public Step3Key(Text w1, Text w2, Text decade) { //, Text c12, Text N
        super();
        this.w1 = w1;
        this.w2 = w2;
        this.decade = decade;
//        this.c12 = c12;
//        this.N = N;
    }
    public Text getW1() {
        return this.w1;
    }
    public Text getW2() {
        return this.w2;
    }
    public Text getDecade() { return this.decade; }
//    public Text getC12() { return this.c12; }
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
//        this.c12.write(out);
//        this.N.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.w1.readFields(in);
        this.w2.readFields(in);
        this.decade.readFields(in);
//        this.c12.readFields(in);
//        this.N.readFields(in);
    }


    @Override
    public int compareTo(Step3Key other) { // w1 > decade
//        if(this.w1.equals(other.getW1())&&other.isFirst())
//            return 0;
//        else
//            return this.w1.compareTo(other.getW1());
//        if(this.isFirst())
//            return 1;
//        if(other.isFirst())
//            return -1;
        if (this.w2.compareTo(other.w2) > 0)
            return 1;
        if (this.w2.compareTo(other.w2) < 0)
            return -1;
//        if (w2.compareTo(other.w2) > 0)
//            return 1;
//        if (w2.compareTo(other.w2) < 0)
//            return -1;
//        if (decade.compareTo(other.decade) > 0)
//            return 1;
//        if (decade.compareTo(other.decade) < 0)
//            return -1;

//        return w2.compareTo(other.w2);
        return 0;
    }
    @Override
    public String toString() {
        return this.w1.toString() + " " + this.w2 + " " + this.getDecade();
    }

}
