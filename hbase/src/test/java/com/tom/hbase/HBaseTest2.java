package com.tom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: HBaseTest2
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/20 10:33
 */
public class HBaseTest2 {
    private Configuration conf = null;
    private Connection conn = null;
    @Before
    public void init(){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop201,hadoop202,hadoop203");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }

    /**
     * 判断表是否存在
     * @throws IOException
     */
    @Test
    public void isExistsTest() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf("student"));
        System.out.println(result);
    }

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        //创建表描述器和列描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("student4"));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        //获取操作hbase的客户端
        Admin admin = conn.getAdmin();
        //关联表和列族
        hTableDescriptor.addFamily(hColumnDescriptor);
        //调用create方法
        admin.createTable(hTableDescriptor);
        System.out.println("创建表成功！");
    }

    /**
     * 删除表
     * @throws IOException
     */
    @Test
    public void deleteTable() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf("student4"));
        admin.deleteTable(TableName.valueOf("student4"));

    }

    /**
     * 插入一条数据，一列
     * @throws IOException
     */
    @Test
    public void insertData() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        Table stu = conn.getTable(TableName.valueOf("student"));
        stu.put(new Put(Bytes.toBytes("1004")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhaoyun")));
    }

    /**
     * 插入一条数据，多列
     * @throws IOException
     */
    @Test
    public void addData() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        Table stu = conn.getTable(TableName.valueOf("student"));
        List<Put> data = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("1005"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("guanyv"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("23"));
        stu.put(put);
    }

    /**
     * 插入多条数据
     * @throws IOException
     */
    @Test
    public void addDatas() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        Table stu = conn.getTable(TableName.valueOf("student"));
        //定义一个存储要插入数据的集合
        List<Put> datas = new ArrayList<>();
        //初始化要插入的数据
        Put put1 = new Put(Bytes.toBytes("1012"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("12"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("北京"));

        Put put2 = new Put(Bytes.toBytes("1011"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("李四"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("21"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("上海"));

        datas.add(put1);
        datas.add(put2);

        stu.put(datas);
        System.out.println("批量插入成功！");
    }

    /**
     * 获取指定一行数据
     * @throws IOException
     */
    @Test
    public void getData() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        Table student = conn.getTable(TableName.valueOf("student"));
        Get get = new Get(Bytes.toBytes("1010"));
        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
        Result result = student.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell:cells){
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("row:"+row+"----family:"+family+"----qualifier:"+qualifier+"----value:"+value);
        }
    }

    /**
     * 获取所有数据
     * @throws IOException
     */
    @Test
    public void findData() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result:scanner){
            Cell[] cells = result.rawCells();
            for (Cell cell:cells){
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("row:"+row+"----family:"+family+"----qualifier:"+qualifier+"----value:"+value);
            }
        }
    }

    /**
     * 删除指定数据
     * @throws IOException
     */
    @Test
    public void deleteData() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("student"));
        Delete delete = new Delete(Bytes.toBytes("1010"));
        //删除指定RowKey的指定列名数据
//        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        //删除指定RowKey的全部数据,只指定RowKey
        table.delete(delete);
    }

    /**
     * 关闭conn
     * @throws IOException
     */
    @After
    public void destroy() throws IOException {
        if (conn!=null){
            conn.close();
        }
    }

}

