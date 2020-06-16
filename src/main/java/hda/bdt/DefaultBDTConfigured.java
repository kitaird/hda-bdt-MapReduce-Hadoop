package hda.bdt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public abstract class DefaultBDTConfigured extends Configured implements Tool {

    private static Class<? extends DefaultBDTConfigured> impl;
    private static Class<? extends Mapper> mapper;
    private static Class<? extends Reducer> reducer;
    private static Class<? extends WritableComparable> writable;


    protected static void setUp(final Class<? extends DefaultBDTConfigured> i,
                                final Class<? extends Mapper> m,
                                final Class<? extends Reducer> r,
                                final Class<? extends WritableComparable> w) {
        impl = i;
        mapper = m;
        reducer = r;
        writable = w;
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
        job.setJarByClass(impl);

        // set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(writable);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set the mapper and reducer class
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);


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
