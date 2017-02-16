/**
 * Created by hzxe on 2/12/17.
 */
import com.sun.xml.internal.rngom.ast.builder.GrammarSection;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class WordGet extends Configured implements Tool {

    public static final int K = 10;
    private static TreeMap<Integer, Text> fatcats = new TreeMap<Integer, Text>();
    private static TreeMap<Integer, Text> results = new TreeMap<Integer, Text>();

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "WordGet"); //利用job取代了jobclient
        job.setJarByClass(WordGet.class);

        Path in = new Path(strings[0]);
        Path out = new Path(strings[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(WordGet.WordMap.class);
        job.setCombinerClass(WordCombine.class);
        job.setReducerClass(WordGet.WordReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);  //此处如果不进行设置，系统会抛出异常，还要记住新旧API不能混用

        System.exit(job.waitForCompletion(true)?0:1);
        return 0;
    }



    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new WordGet(), args);    //调用新的类的方法免除配置的相关琐碎的细节
        System.exit(res);
    }

    static public class WordMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().trim().compareTo("") == 0)
                return;


            int count = 1;

            List<String> result = new ArrayList();

            try {
                Analyzer analyzer = new StandardAnalyzer();
                TokenStream tokenStream = analyzer.tokenStream("field", value.toString().trim());
                CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);
                tokenStream.reset();
                while (tokenStream.incrementToken()) {
                    result.add(term.toString());
                }
                tokenStream.end();
                tokenStream.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            if (!result.isEmpty()) {
                for (String word : result) {
                    Text text = new Text();
                    text.set(word);
                    context.write(text, new IntWritable(count));
                }
            }
        }
    }
    static  public class WordCombine extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum = 0;
            for(IntWritable value :values){
                sum += value.get();
            }
            fatcats.put(sum,key);
            if(fatcats.size() > K){
                fatcats.remove(fatcats.firstKey());
            }

            while(fatcats.size() >0){
                Map.Entry<Integer,Text> entry=fatcats.pollFirstEntry();
                context.write(entry.getValue(),new IntWritable(entry.getKey()));
            }
        }
    }
    static  public class WordReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable value :values){
                sum += value.get();
            }
            results.put(sum,key);
            if(results.size() > K){
                results.remove(results.firstKey());
            }

            while(results.size() >0){
                Map.Entry<Integer,Text> entry=results.pollFirstEntry();
                context.write(entry.getValue(),new IntWritable(entry.getKey()));
            }
        }
    }

}
