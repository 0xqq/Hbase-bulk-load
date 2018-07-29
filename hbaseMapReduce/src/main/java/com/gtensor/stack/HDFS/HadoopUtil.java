package com.gtensor.stack.HDFS;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

    import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.FileStatus;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;

import com.gtensor.stack.readprop.PropReader;


public class HadoopUtil {	
		private static FileSystem fs = null;
		private static Configuration conf = null;
		
		static{
		    conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
			try {
				fs = FileSystem.get(new URI(PropReader.Reader("hdfs_url")),conf);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}		
		}
		 public static boolean uploadFile(String src,String dst) throws IOException{
		        FileSystem fs = FileSystem.get(conf);
		        Path srcPath = new Path(src); 
		        Path dstPath = new Path(dst);
		        fs.copyFromLocalFile(true, srcPath, dstPath);
		        System.out.println("Upload to "+conf.get("fs.defaultFS"));
		        FileStatus[] status = fs.listStatus(dstPath);
		        for(int i=0;i<status.length;i++) {
		        	System.out.println(status[i].getPath().toString());
		        }
		        fs.close();
		        return true;
		    }
	}

