package hda.bdt.impl.adi;

import hda.bdt.DefaultBDTConfigured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MovieTitle1 extends DefaultBDTConfigured implements Tool {

    public static void main(String[] args) throws Exception {
        setUp(MovieTitle1.class, MapperImpl.class, ReducerImpl.class, IntWritable.class);
        int exitCode = ToolRunner.run(new MovieTitle1(), args);
        System.exit(exitCode);
    }

    public static class MapperImpl extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject movieObject = (JSONObject) jsonParser.parse(value.toString());
                JSONArray ratingsArrayObject = (JSONArray) movieObject.get("ratings");
                for (Object o : ratingsArrayObject) {
                    JSONObject ratingsObject = (JSONObject) o;
                    if ((Long) ratingsObject.get("userId") == 10L) {
                        word.set(movieObject.get("title").toString());
                        context.write(word, one);
                    }
                }
            } catch (ParseException ex) {
                Logger.getLogger(RatingCounter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, one);
        }
    }

}

