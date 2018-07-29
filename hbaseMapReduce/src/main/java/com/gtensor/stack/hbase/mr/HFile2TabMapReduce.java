package com.gtensor.stack.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gtensor.stack.readprop.PropReader;
/***
 * 本MapReduce类完成了将导入的csv文件转换为hfile文件
 * 同时调用bulk load将hfile文件载入HBase数据库中
 * @author spark
 *
 */
public class HFile2TabMapReduce extends Configured implements Tool {
    /***
     * 这里主要实现Map类，reduce类HBase已经提供
     * 输入为 long 和 string
     * 输出为 hbase提供的列键和put
     * @author spark
     *
     */
	public static class HFile2TabMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		//列间外置，使列键唯一，这样只用创建一个文件夹
		ImmutableBytesWritable rowkey = new ImmutableBytesWritable();
		/***
		 * map主要完成分片操作
		 */
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//csv格式的文件是  “，”  为分割符
			String[] words = value.toString().split(",");
			//将数据封装给put
			Put put = new Put(Bytes.toBytes(words[0]));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("packettime"),Bytes.toBytes(words[1]));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("srcIP"),Bytes.toBytes(words[2]));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("srcPort"),Bytes.toBytes(words[3]));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("dstIP"),Bytes.toBytes(words[4]));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("srcPort"),Bytes.toBytes(words[5]));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("payloadLen"),Bytes.toBytes(words[6]));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("protoType"),Bytes.toBytes(words[7]));
			rowkey.set(Bytes.toBytes(words[0]));
			//写出给reduce
			context.write(rowkey, put);
		}
	}
	
	public int run(String[] args) throws Exception {
		
		//创建job
		Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());
		
		// 设置jar
		job.setJarByClass(this.getClass());
		
		// 设置input . output
		FileInputFormat.addInputPath(job, new Path(PropReader.Reader("arg1")));
		FileOutputFormat.setOutputPath(job, new Path(PropReader.Reader("arg2")));
		
		// 设置 map
		job.setMapperClass(HFile2TabMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		// 设置reduce
		job.setReducerClass(PutSortReducer.class);
		
		HTable table = new HTable(getConf(), PropReader.Reader("arg0"));
		// 设置hfile output
		HFileOutputFormat2.configureIncrementalLoad(job, table );
		
		// 提交 job
		boolean b = job.waitForCompletion(true);
		if(!b) {
			throw new IOException(" error with job !!!");
		}
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
		// 调用bulk load 载入Hbase
		loader.doBulkLoad(new Path(PropReader.Reader("arg2")), table);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		// 设置 configuration
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",PropReader.Reader("hbase_serverIP")); 
        conf.set("hbase.zookeeper.property.clientPort",PropReader.Reader("hbase_clientport")); 
        conf.set("zookeeper.znode.parent",PropReader.Reader("hbase_parent"));
		//运行job
		int status = ToolRunner.run(conf, new HFile2TabMapReduce(), null);
		
		// exit
		System.exit(status);
	}


}
