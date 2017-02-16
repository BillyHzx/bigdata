/**
 * Created by hzxe on 2/7/17.
 */



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class MaxNum extends Configured implements Tool {

    public static class MyMap  extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
        @Override
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
            if(value.toString().trim().compareTo("")==0)
                return;
            String str=value.toString();
            context.write(new IntWritable(Integer.parseInt(str)),new IntWritable(Integer.parseInt(str)));
        }
    }
    public static class MyReduce  extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        @Override
        public  void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
            int maxnum = 0;

            for(IntWritable value : values)
            {
                if(value.get() > maxnum){
                    maxnum = value.get();
                }
            }
            context.write(new IntWritable(1),new IntWritable(maxnum));
        }
    }

    public int run(String[] args) throws Exception{
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "MaxNum"); //利用job取代了jobclient
        job.setJarByClass(MaxNum.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);  //此处如果不进行设置，系统会抛出异常，还要记住新旧API不能混用

        System.exit(job.waitForCompletion(true)?0:1);
        return 0;

    }



}
