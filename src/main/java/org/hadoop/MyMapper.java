package org.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Nirvana
 * @date 2021/1/26 23:18
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // super.map(key, value, context);

        String[] split = value.toString().split(" ");
        for (String str : split) {
            context.write(new Text(str), new IntWritable(1));
        }
    }
}
