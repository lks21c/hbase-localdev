package com.creamsugardonut;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MyHBaseIntegrationTest {
	private static HBaseTestingUtility utility;
	byte[] CF = "CF".getBytes();
	byte[] QUALIFIER = "qualifier".getBytes();

	@Before
	public void setup() throws Exception {
		utility = new HBaseTestingUtility();
		utility.startMiniCluster();
	}

	@Test
	public void testInsert() throws Exception {
		HTableInterface table = utility.createTable(Bytes.toBytes("MyTest"),
				Bytes.toBytes("CF"));
		
		Put put = new Put("rowkey".getBytes());
		put.add(CF, QUALIFIER, Bytes.toBytes("1"));
		table.put(put);
		
		Result result = table.get(new Get("rowkey".getBytes()));
		result.getColumnCells(CF, QUALIFIER);
		Cell cellValue = result.getColumnLatestCell(Bytes.toBytes("CF"), QUALIFIER);
		String str = Bytes.toString(CellUtil.cloneValue(cellValue));
		Assert.assertEquals("1", str);
	}
}