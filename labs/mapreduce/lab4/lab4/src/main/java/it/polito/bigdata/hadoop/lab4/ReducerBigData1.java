package it.polito.bigdata.hadoop.lab4;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type, key: user ID
        Iterable<IntWritable> values, // Input value type, list of review score for the user
        Context context) throws IOException, InterruptedException {

        //compute score average for each user
        int scoreSum = 0;
        int scoreNum = 0;
        float scoreAvg = 0;

        for(IntWritable score : values){

            scoreSum += score.get();
            scoreNum++;
        }

        scoreAvg = (float) scoreSum / scoreNum;

        context.write(key, new FloatWritable(scoreAvg));
    }
}
