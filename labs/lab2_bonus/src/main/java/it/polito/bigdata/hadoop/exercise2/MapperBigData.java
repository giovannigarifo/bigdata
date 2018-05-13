package it.polito.bigdata.hadoop.exercise2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab 1 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type

    String filter;

    @Override
    protected void setup(Context context) {
        filter = context.getConfiguration().get("filter").toString();
    }

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] v = value.toString().split("\\t");
        String[] bigram = v[0].split("\\s");

        //apply the filter and emit
        if(bigram[0].equals(filter) || bigram[1].equals(filter)){

            context.write(new Text(bigram[0] + " " + bigram[1]), NullWritable.get());
        }
    }
}
