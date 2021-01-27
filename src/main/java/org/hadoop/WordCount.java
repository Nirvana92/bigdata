package org.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author Nirvana
 * @date 2021/1/26 23:07
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();


        Job job = Job.getInstance(config);

        job.setJarByClass(WordCount.class);
//      job.setJarByClass(MyJob.class);
//      // Specify various job-specific parameters
        job.setJobName("wordcount");
//
//      job.setInputPath(new Path("in"));
//      job.setOutputPath(new Path("out"));

        JobConf jobConf = new JobConf(config);
        TextInputFormat.addInputPath(jobConf, new Path("/usr/data/input"));

        Path outputPath = new Path("/usr/data/output");
        // 保证输出文件的路径是唯一存在的，当路径存在进行删除操作
        if(outputPath.getFileSystem(config).exists(outputPath)) {
            outputPath.getFileSystem(config).delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(jobConf, outputPath);

//      job.setMapperClass(MyJob.MyMapper.class);
//      job.setReducerClass(MyJob.MyReducer.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
//      // Submit the job, then poll for progress until the job is complete
//      job.waitForCompletion(true);
        job.waitForCompletion(true);
    }
}
