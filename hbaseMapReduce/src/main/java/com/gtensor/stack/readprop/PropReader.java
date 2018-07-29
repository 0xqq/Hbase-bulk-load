package com.gtensor.stack.readprop;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.gtensor.stack.HDFS.HadoopUtil;

public  class PropReader {
	/***
	 * 配置文件读取，主要不要含有中文目录
	 * @param key
	 * @return
	 */
	public static String Reader(String key){
		Properties prop = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(HadoopUtil.
					class.getClassLoader().getResource("config.properties").getPath()));
			prop.load(in);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String value = prop.getProperty(key);
		return value;
	}

}
