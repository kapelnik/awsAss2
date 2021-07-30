import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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


public class Step2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Step2Key, Step2Value> {
        private final static IntWritable one = new IntWritable(1);
//        private  static Text N  = new Text("0");



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            Text first_word = new Text(itr.nextToken());
            Text second_word = new Text(itr.nextToken());
            Text decade = new Text(itr.nextToken());
            Text c12 = new Text(itr.nextToken());
//            Text N = new Text(itr.nextToken());

            //--------------------------------
            // adding the words into the context

            Step2Value step2Value = new Step2Value(first_word,second_word, decade, c12);
//            Step2Key step2Key = new Step2Key(first_word);
            Step2Key step2Key = new Step2Key(first_word,second_word,decade,c12);
//            if (step2Key.isN()) N = step2Key.getC12();

            context.write(step2Key, step2Value);

        }

    }
//
//
    public static class ReducerClass extends Reducer<Step2Key, Step2Value, Step2Value, Text> {
    private  static Text N  = new Text("");
        private Text c1 ;

    @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //get N from the first line of the last job..
            N=getN( context);
            c1 = new Text("0");
        }

    @Override
    public void reduce(Step2Key key, Iterable<Step2Value> values, Context context) throws IOException, InterruptedException {
        if (!key.isN()) {
                LinkedList<Step2Value> step2Values = new LinkedList<>();
                for (Step2Value other : values) {
                    step2Values.add(other.copy());
                    if (other.isFirst())
                        c1 = new Text(other.getC12());   //this is *(coming first)
                }
                for (Step2Value other : step2Values)
                    if (!(other.isFirst() || other.isSecond()))
                        context.write(other, new Text(N + " " + c1));
        }
        else //this is the key * *, add all second words to context for next job:
            for (Step2Value other : values)
                if (other.isSecond())
                    context.write(other, new Text(N + " " + c1));
    }

    public Text getN(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path path = new Path("/out1/part-r-00000");
        path = path.getFileSystem(conf).makeQualified(path);
        FileSystem hdfs = FileSystem.get(conf);
        InputStream Readfile = hdfs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(Readfile));
        String st = br.readLine();;
        return new Text(st.split("\t")[1]);
    }
}
    public static class PartitionerClass extends Partitioner<Step2Key, Step2Value> {
        @Override
        public int getPartition(Step2Key key, Step2Value step2Value, int numPartitions) {
            return (key.getW1().toString()).hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Step2Key.class);
        job.setMapOutputValueClass(Step2Value.class);
        job.setOutputKeyClass(Step2Value.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true) ;
    }

}
