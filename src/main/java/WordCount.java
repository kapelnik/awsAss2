import java.io.*;
import java.util.StringTokenizer;

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
import java.util.*;
import java.io.IOException;

public class WordCount { 
 
public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable two = new IntWritable(2);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        Text first_word = new Text(itr.nextToken());
        Text second_word = new Text(itr.nextToken());
        ArrayList<Text> stopWords = FileToArray("/home/talaws/Desktop/hebStopWords.txt");
//        System.out.println(Arrays.toString(stopWords.toArray()));
//        System.out.println(first_word+ " " + second_word);
        //--------------------------------
        // Stop Words Check
        if(stopWords.contains(first_word)||stopWords.contains(second_word))
            return;
        //--------------------------------
        // adding the words into the context
        context.write(first_word, one);
        context.write(second_word, one);
        context.write(value, one); // adds the 2 words has a word
        context.write(new Text("#totalSingleWords#"), two);
    }

    public ArrayList<Text> FileToArray(String path) throws IOException {
        ArrayList<Text> result = new ArrayList<>();

        try (FileReader f = new FileReader(path)) {
            StringBuffer sb = new StringBuffer();
            while (f.ready()) {
                char c = (char) f.read();
                if (c == '\n') {
                    result.add(new Text(sb.toString()));
                    sb = new StringBuffer();
                } else {
                    sb.append(c);
                }
            }
            if (sb.length() > 0) {
                result.add(new Text(sb.toString()));
            }
        }
        return result;
    }

}
    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));

        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}