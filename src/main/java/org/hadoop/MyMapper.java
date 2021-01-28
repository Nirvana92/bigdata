package org.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Nirvana
 * @date 2021/1/26 23:18
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    IntWritable count = new IntWritable(1);
    Text str = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // super.map(key, value, context);

//        String[] split = value.toString().split(" ");
//        for (String str : split) {
//            context.write(new Text(str), count);
//        }
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            str.set(tokenizer.nextToken());
            context.write(str, count);
        }
    }
}
