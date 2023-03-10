//Grace Meredith
//CS433 HW1 Q1 find top 25 hashtags

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
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;
import java.lang.String;

public class Q1 {

  public static class Mapper_Hashtags extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        String isHashtag = word.toString();
        if(isHashtag.matches("#(?!\\d)\\w*")) {
          context.write(word, one);
        }
      }
    }
  }

  public static class Reducer_Top_25 extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private TreeMap<Integer, String> top_25;
  
    public void setup(Context context) throws IOException, InterruptedException  {
      top_25 = new TreeMap<Integer, String>();
    }

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      top_25.put(sum, key.toString());
    }
 
    public void cleanup(Context context) throws IOException, InterruptedException {
      while(top_25.size() > 25) {
        top_25.remove(top_25.firstKey());
      }

      for(Map.Entry<Integer, String> entry : top_25.entrySet()) {
        int count = entry.getKey();
        String name = entry.getValue();
	      result.set(count);
        context.write(new Text(name), result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Q1.class);
    job.setMapperClass(Mapper_Hashtags.class);
    job.setCombinerClass(Reducer_Top_25.class);
    job.setReducerClass(Reducer_Top_25.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
