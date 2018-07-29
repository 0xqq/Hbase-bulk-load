package com.gtensor.stack.hbase.oper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.IOUtils;

import com.gtensor.stack.readprop.PropReader;

public class HBaseOper{
	//获得Hbase已经封装好的操作类
	private static HBaseAdmin admin = null;
	//设置配置
	private static Configuration conf;
	//设置配置文件
      static {
		System.out.println("connection");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",PropReader.Reader("hbase_serverIP")); 
        conf.set("hbase.zookeeper.property.clientPort",PropReader.Reader("hbase_clientport"));
        conf.set("zookeeper.znode.parent",PropReader.Reader("hbase_parent"));
        System.out.println("success");
        try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
      
     /***
      * 获得表
      * @return
      */
	 public static  List getAllTables() {
	        List<String> tables = null;
	        System.out.println("select");
	        if (admin != null) {
	            try {
	                HTableDescriptor[] allTable = admin.listTables();
	                if (allTable.length > 0)
	                    tables = new ArrayList<String>();
	                for (HTableDescriptor hTableDescriptor : allTable) {
	                    tables.add(hTableDescriptor.getNameAsString());
	                    System.out.println(hTableDescriptor.getNameAsString());
	                }
	            }catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	        return tables;
	    }
    /***
     * 测试过滤器
     */
	public static void testFilter() {
		HTable table = null;
		try {
			table = getTable();
			Scan scan = new Scan();
	        
			Filter filter = null;
			filter = new PrefixFilter(Bytes.toBytes("rk"));
			filter = new PageFilter(3);
			ByteArrayComparable comp = null;
			comp = new SubstringComparator("lisi");
			filter = new SingleColumnValueFilter(
					Bytes.toBytes("info"),
					Bytes.toBytes("name"), 
					CompareOp.EQUAL, 
					comp);
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs) {
				printResult(result);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(table != null) {
				IOUtils.closeStream(table);
			}
		}
	}
	/***
	 * 查看数据
	 */
	public static void scanData() {
		HTable table = null;
		try {
			table = getTable();
			Scan scan = new Scan();
			
			scan.setStartRow(Bytes.toBytes("rk0001"));
			scan.setStopRow(Bytes.toBytes("rk0003"));
			
			scan.addFamily(Bytes.toBytes("info"));
			scan.setCacheBlocks(false);
			scan.setBatch(2);
			scan.setCaching(2);
			ResultScanner rs = table.getScanner(scan);
			
			for(Result result : rs) {
				printResult(result);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(table != null) {
				IOUtils.closeStream(table);
			}
		}
	}
	/***
	 * 输出结果
	 * @param rs
	 */
	public static void printResult(Result rs) {
		for (Cell cell : rs.rawCells()) {
			System.out.println(
					Bytes.toString(CellUtil.cloneFamily(cell)) + " : " +
					Bytes.toString(CellUtil.cloneRow(cell)) + " : " +
					Bytes.toString(CellUtil.cloneQualifier(cell)) + " : " +
					Bytes.toString(CellUtil.cloneValue(cell))
			);
		}
	}
	
	
	public static void testNamespace() throws Exception {
		Configuration c = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(c );
		NamespaceDescriptor namespace = NamespaceDescriptor.create("ns1").build();
		admin.deleteNamespace("ns1");
		admin.close();
	}
	/**
	 * 使用get得到数据
	 * @throws Exception
	 */
	public static void getData() throws Exception {
		HTable table = getTable();
		Get get = new Get(Bytes.toBytes("rk0001"));
		Result result = table.get(get );
		
		for (Cell cell : result.rawCells()) {
			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}
    /***
     * 使用put添加数据
     * @throws Exception
     */
	public static void putData() throws Exception {
		//get table
		HTable table = getTable();
		Put put =new Put(Bytes.toBytes("rk0001"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
		
		table.put(put);
		table.close();
	}
	/***
	 * 得到表
	 * @return
	 * @throws Exception
	 */
	public static HTable getTable() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, Bytes.toBytes("traffic_statistics"));
		return table;
	}
    /***
     * 创建表
     * @throws Exception
     */
	public static void createTab() throws Exception {
		boolean b = admin.tableExists(Bytes.toBytes("traffic_statistics"));
		System.out.println(b);
		if (b) {
			admin.disableTable(Bytes.toBytes("traffic_statistics"));
			admin.deleteTable("traffic_statistics");
		}
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("traffic_statistics"));

		table.addFamily(new HColumnDescriptor(Bytes.toBytes("info")));
		admin.createTable(table);

		admin.close();
	}
	/***
	 * main函数
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("start");
		try {
		
		System.out.println("connection");
		createTab();
		System.out.println("create success");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}