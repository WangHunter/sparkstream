import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class hbase_test {



    // 声明静态配置
    private static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "coreserver05.zbjt.com,dwdb01.zbjt.com,hue.zbjt.com");
//        conf.set("hbase.zookeeper.quorum", "coreserver05.zbjt.com");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.zookeeper.quorum","10.100.63.62");
//        conf.set("hbase.master", "10.100.63.55:60000");
    }

    // 创建数据库表
    public static void createTable(String tableName, String[] columnFamilys) throws IOException {
        // 建立一个数据库的连接
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName userTable  = TableName.valueOf(tableName);
        HTableDescriptor tableDescr = new HTableDescriptor(userTable);
        tableDescr.addFamily(new HColumnDescriptor(columnFamilys[1].getBytes()));
        Admin admin = conn.getAdmin();


        if (admin.tableExists(userTable)) {
            System.out.println(tableName + "表已存在");
            conn.close();
            System.exit(0);
        } else {
            // 新建一个表描述
            admin.createTable(tableDescr);
            System.out.println("创建" + tableName + "表成功");
        }
        conn.close();
    }

    // 添加一条数据
    public static void addRow(String tableName, String rowKey, String columnFamily, String column, String value)
            throws IOException {
        // 建立一个数据库的连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        // 通过rowkey创建一个put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 在put对象中设置列族、列、值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        // 插入数据,可通过put(List<Put>)批量插入
        table.put(put);
        // 关闭资源
        table.close();
        conn.close();
    }



    // 通过rowkey获取一条数据
    public static void getRow(String tableName, String rowKey) throws IOException {
        // 建立一个数据库的连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        // 通过rowkey创建一个get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 输出结果
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    "行键:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                            "列族:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                            "列名:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                            "值:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                            "时间戳:" + cell.getTimestamp());
        }
        // 关闭资源
        table.close();
        conn.close();
    }
    public static void main(String[] args) {
        String tablename="test3";
        String [] columnFamilys= {"c1","c2"};

        try {
            addRow(tablename,"test","f","f","100");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("创建表:"+tablename+"失败。。。。");

        }

//        try {
//            //addRow(tablename,"1","c1","name","test3");
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.out.println("往表:"+tablename+"插入数据失败。。。。");
//        }


    }





}
