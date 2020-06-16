package hda.bdt;


import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
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

public class AvgRatingCounter extends Configured implements Tool {
    public static class MoviesMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final static DoubleWritable one = new DoubleWritable(1.0);

        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject movieObject = (JSONObject) jsonParser.parse(value.toString());
                Object movieId = movieObject.get("_id");
                Object movieTitle = movieObject.get("title");
                JSONArray ratingsArrayObject = (JSONArray) movieObject.get("ratings");
                Iterator i = ratingsArrayObject.iterator();
                while (i.hasNext()) {
                    JSONObject ratingsObject = (JSONObject) i.next();
                    word.set("SumRatings");
                    one.set(Double.parseDouble(ratingsObject.get("rating").toString()));
                    context.write(new Text(movieTitle.toString()), one);
                }
            } catch (ParseException ex) {
                Logger.getLogger(AvgRatingCounter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class RatingsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int i = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                i++;
            }
            result.set(sum/i);
            if((sum/i) >= 4.0) {
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvgRatingCounter(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        // when implementing tool
        Configuration conf = this.getConf();

        // create job
        Job job = Job.getInstance(conf, "RatingCounter");
        job.setJarByClass(AvgRatingCounter.class);

        // set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set the mapper and reducer class
        job.setMapperClass(MoviesMapper.class);
        job.setReducerClass(RatingsReducer.class);


        // wait for the job to finish
        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        // monitor & output execution time
        if (job.isSuccessful()) {
            System.out.println("Job was successful");
            System.out.println("Time: " + String.valueOf(job.getStartTime() - job.getFinishTime()));
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }
        return returnValue;
    }


}
