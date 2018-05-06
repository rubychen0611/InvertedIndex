import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.*;

public class InvertedIndex
{
    private static String tableName = "Wuxia";
    private static Configuration HBASE_CONFIG = HBaseConfiguration.create();
    //private static HTable table;
    private static MultipleOutputs<Text, Text> mos;
    private static Path outputPath;
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();    //得到文件名
            fileName = fileName.replace(".txt.segmented", "");  //去掉.txt.segmented
            fileName = fileName.replace(".TXT.segmented", "");  //去掉.TXT.segmented
            fileName = fileName.replace(".", "");  //去掉.
            String temp;
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                temp = itr.nextToken();
                word.set(temp + "," + fileName);
                context.write(word, new IntWritable(1));
            }
        }
    }
    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for(IntWritable val: values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static class NewPartitioner extends HashPartitioner<Text, IntWritable>
    {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks)
        {
            String term = key.toString().split(",")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }
    public static class InvertedIndexReducer extends TableReducer<Text, IntWritable, NullWritable>
    {
        private Text word1 = new Text(), word2 = new Text();
        private String temp = new String();
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

       /* protected void setup(Context context)
        {
            mos = new MultipleOutputs(context);
        }*/
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            word1.set(key.toString().split(",")[0]);        //word
            temp = key.toString().split(",")[1];            //filename
            for(IntWritable val: values)
                sum += val.get();
            word2.set(temp + ":" + sum);
            if(!CurrentItem.equals(word1) && !CurrentItem.equals(" "))   //新的单词，统计前面所有词频
            {
                StringBuilder out = new StringBuilder();
                long count = 0;
                int num = 0;
                for(String p: postingList)
                {
                    num++;
                    out.append(p);
                    out.append(";");
                    count += Long.parseLong(p.substring(p.indexOf(":") + 1));
                }
                if(count > 0)
                {
                    double avg_num = (double)count / num;
                    /*向表中添加数据*/
                    Put put = new Put(Bytes.toBytes(CurrentItem.toString()));
                    put.add(Bytes.toBytes("f1"), Bytes.toBytes("AVG_NUM"), Bytes.toBytes(avg_num));
                    context.write(NullWritable.get(), put);
                   // HTable table = new HTable(HBASE_CONFIG, tableName);
                   // table.put(put);
                   // table.close();
                    /*文件输出倒排索引结果*/
                   // String avg = new Formatter().format("%.1f", avg_num).toString();
                    //context.write(CurrentItem, new Text(avg + "," + out.toString()));
                  //  mos.write("hdfs", CurrentItem, new Text(avg + "," + out.toString()), outputPath.toString());
                }
                postingList = new ArrayList<String>();
                CurrentItem = new Text(word1);
            }
            postingList.add(word2.toString());  //旧的单词，添加进postingList
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            StringBuilder out = new StringBuilder();
            long count = 0;
            int num = 0;
            for(String p: postingList)
            {
                num++;
                out.append(p);
                out.append(";");
                count += Long.parseLong(p.substring(p.indexOf(":") + 1));
            }
            if(count > 0)
            {
                double avg_num = (double)count / num;
                /*向表中添加数据*/
                Put put = new Put(Bytes.toBytes(CurrentItem.toString()));
                put.add(Bytes.toBytes("f1"), Bytes.toBytes("AVG_NUM"), Bytes.toBytes(avg_num));
                context.write(NullWritable.get(), put);
                // HTable table = new HTable(HBASE_CONFIG, tableName);
                // table.put(put);
                // table.close();
                /*文件输出倒排索引结果*/
               // String avg = new Formatter().format("%.1f", avg_num).toString();
                //context.write(CurrentItem, new Text(avg + "," + out.toString()));
               // mos.write("hdfs", CurrentItem, new Text(avg + "," + out.toString()), outputPath.toString());
            }
            //table.close();//释放资源
           // mos.close();
        }
    }
    public static void main(String[] args)
    {

        try{
            HBASE_CONFIG.set(TableOutputFormat.OUTPUT_TABLE, tableName);
            HBASE_CONFIG.set("dfs.socket.timeout", "180000");

            HBaseAdmin hAdmin = new HBaseAdmin(HBASE_CONFIG);
            if(hAdmin.tableExists(tableName))       //hbase中已存在表,删除
            {
                System.out.println("table " + tableName + " already exists!");
                hAdmin.disableTable(tableName);
                hAdmin.deleteTable(tableName);
            }
            //创建新表
            HTableDescriptor t = new HTableDescriptor(tableName);
            t.addFamily(new HColumnDescriptor("f1"));
            hAdmin.createTable(t);
            //table = new HTable(HBASE_CONFIG, tableName);
            //table = new HTable(HBASE_CONFIG, Bytes.toBytes(tableName));
           // HTablePool pool = new HTablePool(HBASE_CONFIG, 1000);
           // table = (HTable) pool.getTable(tableName);

            System.out.println("Create table "+ tableName + " success!");


            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf,"InvertedIndex");

            job.setJarByClass(InvertedIndex.class);

            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(SumCombiner.class);
            //job.setPartitionerClass(NewPartitioner.class);
            TableMapReduceUtil.initTableReducerJob(tableName, InvertedIndexReducer.class, job,NewPartitioner.class,null,null,null,false);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(Put.class);
            //job.setReducerClass(InvertedIndexReducer.class);
            //job.setOutputKeyClass(Text.class);
            //job.setOutputValueClass(Text.class);
            //job.setOutputKeyClass(Text.class);
           // job.setOutputValueClass(NullWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TableOutputFormat.class);


            //MultipleOutputs.addNamedOutput(job, "hdfs", TextOutputFormat.class, Text.class, Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            outputPath = new Path(args[1]);
            //FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
