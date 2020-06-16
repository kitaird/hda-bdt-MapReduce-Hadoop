package hda.bdt.impl.adi;

import hda.bdt.DefaultBDTConfigured;
import org.apache.hadoop.io.DoubleWritable;
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

public class MovieTitleAvg1 extends DefaultBDTConfigured implements Tool {

    public static void main(String[] args) throws Exception {
        setUp(MovieTitleAvg1.class, MapperImpl.class, ReducerImpl.class, DoubleWritable.class);
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
                Logger.getLogger(RatingCounter.class.getName()).log(Level.SEVERE, null, ex);
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
}
