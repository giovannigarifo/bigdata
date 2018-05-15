package it.polito.bigdata.hadoop.exercise3;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.polito.bigdata.hadoop.exercise3.TopKVector;
import it.polito.bigdata.hadoop.exercise3.WordCountWritable;


/**
 * Lab 3v1 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type


    private TopKVector<WordCountWritable> tophundred;

    @Override
    protected void setup(Context context) {

         tophundred = new TopKVector<>(100);
    }


    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        int total = 0;

        // Iterate over the set of values and sum them to obtain the total amount of time that productA and productB
        // have been bought together
        for (IntWritable value : values) {
            total = total + value.get();
        }

        WordCountWritable wcw = new WordCountWritable();
        wcw.setWord(key.toString());
        wcw.setCount(total);

        tophundred.updateWithNewElement(wcw);
    }


    @Override
    protected void cleanup(Context context) {

        Vector<WordCountWritable> v = tophundred.getLocalTopK();

        for(WordCountWritable wcw : v) {

            try {

                context.write(new Text(wcw.getWord()), new IntWritable(wcw.getCount()));

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
