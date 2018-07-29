package com.gtensor.stack.HDFS;

import java.io.IOException;

import com.gtensor.stack.readprop.PropReader;

public class HadoopOper { 
	
	 public static void main(String[] args) {
		
		 try {
			//直接在配置文件中设置传入数据的源地址和目的地址即可
			HadoopUtil.uploadFile(PropReader.Reader("src_csv"),PropReader.Reader("dst_scv"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
