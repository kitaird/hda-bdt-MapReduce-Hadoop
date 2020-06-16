package hda.bdt.impl.adi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MovieTitle2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MovieTitle2(), args);
        System.exit(exitCode);
    }

    public static class MapperImpl extends Mapper<Object, Text, Text, IntWritable> {
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject movieObject = (JSONObject) jsonParser.parse(value.toString());
                JSONArray ratingsArrayObject = (JSONArray) movieObject.get("ratings");
                for (Object o : ratingsArrayObject) {
                    JSONObject ratingsObject = (JSONObject) o;
                    word.set(movieObject.get("title").toString());
                    context.write(word, new IntWritable(((Long) ratingsObject.get("userId")).intValue()) );
                }
            } catch (ParseException ex) {
                Logger.getLogger(MovieTitle2.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            AtomicBoolean containsUserId10 = new AtomicBoolean(false);
            values.forEach(v -> {
                if(v.get() == 10){
                    containsUserId10.set(true);
                }
            });
            if(containsUserId10.get()){
                context.write(key, one);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files%n", getClass().getSimpleName());
            return -1;
        }
        // when implementing tool
        Configuration conf = this.getConf();

        // create job
        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJarByClass(MovieTitle2.class);

        // set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set the mapper and reducer class
        job.setMapperClass(MapperImpl.class);
        job.setReducerClass(ReducerImpl.class);


        // wait for the job to finish
        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        // monitor & output execution time
        if (job.isSuccessful()) {
            System.out.println("Job was successful");
            System.out.println("Time: " + (job.getStartTime() - job.getFinishTime()));
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }
        return returnValue;
    }

}
