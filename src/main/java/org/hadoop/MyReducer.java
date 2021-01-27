package org.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Nirvana
 * @date 2021/1/26 23:19
 */
public class MyReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // super.reduce(key, values, context);
        IntWritable sums = new IntWritable();

        for (IntWritable value : values) {
            sums.set(sums.get() + value.get());
        }

        context.write(key, sums);
    }
}
