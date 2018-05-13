package it.polito.bigdata.hadoop.exercise2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab 1 - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type

    String filter_strPrefix;

    @Override
    protected void setup(Context context) {
        filter_strPrefix = context.getConfiguration().get("strPrefix").toString();
    }

    @Override
    protected void map(

            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            if(key.toString().startsWith(filter_strPrefix)){
                context.write(key, value);
            }
    }
}
