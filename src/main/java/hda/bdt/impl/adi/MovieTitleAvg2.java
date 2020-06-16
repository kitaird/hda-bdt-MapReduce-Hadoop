package hda.bdt.impl.adi;

import hda.bdt.DefaultBDTConfigured;
import org.apache.hadoop.io.DoubleWritable;
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

public class MovieTitleAvg2 extends DefaultBDTConfigured implements Tool {

    public static void main(String[] args) throws Exception {
        setUp(MovieTitleAvg2.class, MapperImpl.class, ReducerImpl.class, IntWritable.class);
        int exitCode = ToolRunner.run(new MovieTitleAvg2(), args);
        System.exit(exitCode);
    }

    public static class MapperImpl extends Mapper<Object, Text, Text, IntWritable> {
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject movieObject = (JSONObject) jsonParser.parse(value.toString());
                JSONArray ratingsArrayObject = (JSONArray) movieObject.get("ratings");
                word.set(movieObject.get("title").toString());
                for(Object o: ratingsArrayObject){
                    Long rating = (Long)((JSONObject) o).get("rating");
                    context.write(word, new IntWritable(rating.intValue()));
                }
            } catch (ParseException ex) {
                Logger.getLogger(RatingCounter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double counter = 0;
            for (IntWritable val : values) {
                sum += val.get();
                counter++;
            }
            result.set(sum/counter);
            if(result.compareTo(new DoubleWritable(4)) > -1){
                context.write(key, result);
            }
        }
    }
}
