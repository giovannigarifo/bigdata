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
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        int numNormalizedScores = 0;
        float sumNormalizedScores = 0;

        for(FloatWritable value : values){

            sumNormalizedScores+= value.get();
            numNormalizedScores++;
        }

        float avgNormalizedScore = (float) sumNormalizedScores/numNormalizedScores;

        context.write(key, new FloatWritable(avgNormalizedScore));
    }
}
