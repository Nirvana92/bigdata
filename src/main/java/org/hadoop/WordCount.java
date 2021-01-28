package org.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author Nirvana
 * @date 2021/1/26 23:07
 *
 * 编写完成之后将项目打包, 上传到服务器通过hadoop jar 的方式进行调用
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration config = new Configuration();

        // 参数个性化. 这个里面会处理掉 -D 的配置参数
        GenericOptionsParser parser = new GenericOptionsParser(config, args);
        String[] others = parser.getRemainingArgs();

        //config.set("mapreduce.app-submission.cross-platform", "true");
        config.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(config);
        //job.setJar("/Users/mac/Documents/GitHub/bigdata/target/bigdata-1.0-SNAPSHOT.jar");
        job.setJarByClass(WordCount.class);
//      job.setJarByClass(MyJob.class);
//      // Specify various job-specific parameters
        job.setJobName("wordcount");
//
//      job.setInputPath(new Path("in"));
//      job.setOutputPath(new Path("out"));

        TextInputFormat.addInputPath(job, new Path("/usr/data/input"));

        Path outputPath = new Path("/usr/data/output");
        // 保证输出文件的路径是唯一存在的，当路径存在进行删除操作
        if(outputPath.getFileSystem(config).exists(outputPath)) {
            outputPath.getFileSystem(config).delete(outputPath, true);
        }

        TextOutputFormat.setOutputPath(job, outputPath);

//      job.setMapperClass(MyJob.MyMapper.class);
//      job.setReducerClass(MyJob.MyReducer.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
//      // Submit the job, then poll for progress until the job is complete
//      job.waitForCompletion(true);
        job.waitForCompletion(true);
    }
}
