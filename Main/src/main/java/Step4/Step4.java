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


public class Step4 {

    public static class MapperClass extends Mapper<LongWritable, Text, Step4Key, Step4Value> {
        private final static IntWritable one = new IntWritable(1);
//        private  static Text N  = new Text("0");



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            Text first_word = new Text(itr.nextToken());
            Text second_word = new Text(itr.nextToken());
            Text decade = new Text(itr.nextToken());
            Text npmi = new Text(itr.nextToken());

            //--------------------------------
            // adding the words into the context

            Step4Value step4Value = new Step4Value(first_word,second_word, decade, npmi);
//            Step4Key step4Key = new Step4Key(first_word);
            Step4Key step4Key = new Step4Key(first_word,second_word, decade, npmi);
//            if (step2Key.isN()) N = step2Key.getC12();

            context.write(step4Key, step4Value);

        }

    }
//
//
    public static class ReducerClass extends Reducer<Step4Key, Step4Value, Step4Key, Text> {

    private IntWritable c2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        c2 = new IntWritable(0);
    }

    @Override
    public void reduce(Step4Key key, Iterable<Step4Value> values, Context context) throws IOException, InterruptedException {
                context.write(key,new Text(""));
    }
}
    public static class PartitionerClass extends Partitioner<Step4Key, Step4Value> {
        @Override
        public int getPartition(Step4Key key, Step4Value step4Value, int numPartitions) {
//            return 0;
                return (Integer.parseInt(key.getDecade().toString())+Integer.parseInt(key.getDecade().toString())+Integer.parseInt(key.getW2().toString())) % numPartitions;

        }//TODO think about
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step 4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Step4Key.class);
        job.setMapOutputValueClass(Step4Value.class);
        job.setOutputKeyClass(Step4Value.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
