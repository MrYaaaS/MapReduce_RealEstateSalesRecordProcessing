package org.example;

import com.opencsv.exceptions.CsvValidationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Main {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private DataParser dataParser;
        int targetYear = 2019;
        String targetState = "washington";


        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dataParser = new DataParser();
        }

        private static boolean isNullString(String str) {
            if ((str == null) || (str.trim().length() == 0)) {
                return true;
            } else {
                return false;
            }
        }
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Record record = null;
            try {
                record = dataParser.getSingleRecord(value.toString());
            } catch (CsvValidationException e) {
                throw new RuntimeException(e);
            }

            if(record.getYear() == targetYear && record.getState().toLowerCase().equals(targetState)) {
                outKey.set(record.getPropertyType());
                outValue.set(String.valueOf(record.getPropertyTypeId()));
                context.getCounter(PropertyTypeCounters.PROPERTY_SUM).increment(1);
                context.write(outKey, outValue);
            }
        }
    }

    public static class MyPartitinoer extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // There's 5 types of property within the dataset, partition will be based on propertyTypeId.
            int propertyTypeId = Integer.parseInt(value.toString());
            if(propertyTypeId == 3){
                return 0;
            } else if (propertyTypeId == 4){
                return 1;
            } else if (propertyTypeId == 6){
                return 2;
            } else if (propertyTypeId == 13){
                return 3;
            } else {
                return  4;
            }
        }
    }

    public static class FlightReducer extends
            Reducer<Text, Text, Text, Text> {
        long total;
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            total = currentJob.getCounters().findCounter(PropertyTypeCounters.PROPERTY_SUM).getValue();
        }
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IndexOutOfBoundsException, IOException, InterruptedException {
            int counter = 0;
            for(Text v : values){
                counter++;
            }

            float keyPercentage = (float) counter / (float) total;

            result.set(String.valueOf(keyPercentage * 100) + "%");
            context.write(key, result);

    }

}
    public enum PropertyTypeCounters {
        TOWNHOUSE,
        CONDO,
        SINGLE_FAMILY,
        ALL_RESIDENTIAL,
        MULTI_FAMILY,
        PROPERTY_SUM
    };
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Error on args");
            System.exit(1);
        }

        Job job = new Job(conf, "Percentages of each property type");
        job.setJarByClass(Main.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setPartitionerClass(MyPartitinoer.class);
        job.setReducerClass(FlightReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(5);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
        if (job.isSuccessful()) {
            System.out.println("Finished!!!!!!");
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}