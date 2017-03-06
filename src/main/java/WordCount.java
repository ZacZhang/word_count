import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

// delete output file before execution
public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // key: offset
            // value: line 读进来的一句话
            // example value: I love data

            // split into words
            String[] words = value.toString().split(" ");
            for (String word: words) {
                // I 1
                // love 1
                // data 1
                Text outputKey = new Text(word);// key is the word
                IntWritable outputValue = new IntWritable(1);// 先假设每个单词出现一次，最后再相加，统计工作由reducer完成
                context.write(outputKey, outputValue);
            }

        }
    }

    public static class WordNumberReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // I <1>
            // love <1,1> if love appears twice
            int sum = 0;
            for (IntWritable value: values) {
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(WordNumberReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // 通过命令行来设置输入输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true); // 等待上一个mapreduce结束才能进行下一个mapreduce
    }
}