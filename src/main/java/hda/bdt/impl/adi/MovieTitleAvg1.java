package hda.bdt.impl.adi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.logging.Level;
import java.util.logging.Logger;

public class MovieTitleAvg1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MovieTitleAvg1(), args);
        System.exit(exitCode);
    }

    public static class MapperImpl extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject movieObject = (JSONObject) jsonParser.parse(value.toString());
                JSONArray ratingsArrayObject = (JSONArray) movieObject.get("ratings");
                word.set(movieObject.get("title").toString());
                double avgRating = ratingsArrayObject.stream().map(o -> ((JSONObject) o).get("rating")).mapToLong(Long.class::cast).average().orElse(0);
                context.write(word, new DoubleWritable(avgRating));
            } catch (ParseException ex) {
                Logger.getLogger(MovieTitleAvg1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            if(result.compareTo(new DoubleWritable(4)) > -1){
                context.write(key, result);
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
        job.setJarByClass(MovieTitleAvg1.class);

        // set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
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
