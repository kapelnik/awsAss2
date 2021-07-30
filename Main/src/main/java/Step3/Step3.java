import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;


public class Step3 {

    public static class MapperClass extends Mapper<LongWritable, Text, Step3Key, Step3Value> {
        private final static IntWritable one = new IntWritable(1);
//        private  static Text N  = new Text("0");



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            Text first_word = new Text(itr.nextToken());
            Text second_word = new Text(itr.nextToken());
            Text decade = new Text(itr.nextToken());
            Text c12 = new Text(itr.nextToken());
            Text N = new Text(itr.nextToken());
            Text c1 = new Text(itr.nextToken());

            //--------------------------------
            // adding the words into the context

            Step3Value step3Value = new Step3Value(first_word,second_word, decade, c12, c1, N);
//            Step3Key step3Key = new Step3Key(first_word);
            Step3Key step3Key = new Step3Key(first_word,second_word,decade);
//            if (step2Key.isN()) N = step2Key.getC12();

            context.write(step3Key, step3Value);

        }

    }
//
//
    public static class ReducerClass extends Reducer<Step3Key, Step3Value, Step3Key, Text> {

    private IntWritable c2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        c2 = new IntWritable(0);
    }

    @Override
    public void reduce(Step3Key key, Iterable<Step3Value> values, Context context) throws IOException, InterruptedException {

        LinkedList<Step3Value> step3Values = new LinkedList<>();
        for (Step3Value other : values) {
            step3Values.add(other.copy());
            if (other.isSecond()) {
                c2 = new IntWritable(Integer.parseInt(other.getC12().toString()));   //this is *(coming first)
            }
        }
        for (Step3Value other : step3Values) {
            if (!(other.isFirst() || other.isSecond())) {
                System.out.println("output: " + other.getW1() + " " + other.getW2() + "  " + other.getC12() + " " + other.getC1() + " " + c2 + " " + other.getN() + " " + other.getDecade());
                double npmi = calcNPMI(Double.parseDouble(other.getC1().toString()), Double.parseDouble(c2.toString()),
                        Double.parseDouble(other.getC12().toString()), Double.parseDouble(other.getN().toString()));
                System.out.println("npmi:" + npmi);
                context.write(new Step3Key(other.getW1(), other.getW2(), other.getDecade()), new Text(Double.toString(npmi)));
            }
        }
    }
    private double calcNPMI(double c1, double c2, double c12, double N){
        double p12, pmi;
        p12 = c12 / N;
        pmi=Math.log(c12)+Math.log(N)-Math.log(c1)-Math.log(c2);
        return pmi/(-Math.log(p12));
    }
}
    public static class PartitionerClass extends Partitioner<Step3Key, Step3Value> {
        @Override
        public int getPartition(Step3Key key, Step3Value step3Value, int numPartitions) {
            return (key.getW2().toString()).hashCode() % numPartitions;
//            return (key.getDecade().toString()+key.getW1().toString()).hashCode() % numPartitions;
//            return (Integer.parseInt(key.getDecade().toString())+Integer.parseInt(key.getW1().toString())) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step 3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Step3Key.class);
        job.setMapOutputValueClass(Step3Value.class);
        job.setOutputKeyClass(Step3Value.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}
