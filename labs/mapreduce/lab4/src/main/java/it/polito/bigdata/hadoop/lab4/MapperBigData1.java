package it.polito.bigdata.hadoop.lab4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] splittedReview = value.toString().split(",");

        //if not header of the csv emit the values
        if(!splittedReview[0].equals("Id")){

            //emit (UID,Score)
            context.write(new Text(splittedReview[2]), new IntWritable(Integer.valueOf(splittedReview[6])) );
        }
    }
}
