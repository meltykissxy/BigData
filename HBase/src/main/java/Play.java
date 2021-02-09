import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class Play {
    /**
     * 创建命名空间
     */
    @Test
    public void createNamespace() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        // 获取管理员对象（权限）
        Connection connection = ConnectionFactory.createConnection(cfg);
        final Admin admin = connection.getAdmin();

        NamespaceDescriptor nd = NamespaceDescriptor.create("test").build();
        try {
            admin.createNamespace(nd);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已存在！");
        } catch (Exception e) {
            e.printStackTrace();
        }

        admin.close();
        connection.close();
    }

    /**
     * 判断表是否存在
     */
    @Test
    public void tableExists() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        Connection connection = ConnectionFactory.createConnection(cfg);
        final Admin admin = connection.getAdmin();

        System.out.println(admin.tableExists(TableName.valueOf("Student")));

        admin.close();
        connection.close();
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        Connection connection = ConnectionFactory.createConnection(cfg);
        final Admin admin = connection.getAdmin();

        String tn = "Beauty:emp"; //命名空间:表名
        String[] cfs = {"info", "detail"};

        TableDescriptorBuilder td = TableDescriptorBuilder.newBuilder(TableName.valueOf(tn));
        for ( String cf : cfs ) {
            final ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder
                    = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            td.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        admin.createTable(td.build());

        admin.close();
        connection.close();

    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        Connection connection = ConnectionFactory.createConnection(cfg);
        final Admin admin = connection.getAdmin();

        String tn = "emp";
        TableName name = TableName.valueOf(tn);

        admin.disableTable(name);
        admin.deleteTable(name);

        admin.close();
        connection.close();
    }
    /**
     * 插入数据
     */
    @Test
    public void put() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        Connection connection = ConnectionFactory.createConnection(cfg);
        Table table = connection.getTable(TableName.valueOf("test:student"));

        String rowkey = "1003";
        // 创建Put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        // 增加数据列，可以一次增加多条数据
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("meltykiss"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("meltykiss"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("meltykiss"));
        // 插入数据
        table.put(put);

        table.close();
        connection.close();

    }
    /**
     * 单条数据查询
     */
    @Test
    public void get() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        Connection connection = ConnectionFactory.createConnection(cfg);
        Table table = connection.getTable(TableName.valueOf("student"));

        // 创建Get对象
        Get get = new Get(Bytes.toBytes("1002"));
        // 指定列族查询
        // get.addFamily(Bytes.toBytes(cf));
        // 指定列族:列查询
        // get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        // 查询数据
        Result result = table.get(get);

        // 解析result
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:" + Bytes.toString(cell.getFamilyArray()) +
                    ",CN:" + Bytes.toString(cell.getQualifierArray()) +
                    ",Value:" + Bytes.toString(cell.getValueArray()));
        }

        table.close();
        connection.close();

    }
    /**
     * 扫描数据
     */
    @Test
    public void scan() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        Connection connection = ConnectionFactory.createConnection(cfg);
        Table table = connection.getTable(TableName.valueOf("student"));

        // 创建扫描对象
        Scan scan = new Scan();
        // 获取扫描结果
        ResultScanner results = table.getScanner(scan);
        // 解析results
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("CF:" + Bytes.toString(cell.getFamilyArray()) +
                        ",CN:" + Bytes.toString(cell.getQualifierArray()) +
                        ",Value:" + Bytes.toString(cell.getValueArray()));
            }
        }
        results.close();
        table.close();
        connection.close();

    }
    /**
     * 删除数据
     */
    @Test
    public void delete() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection connection = ConnectionFactory.createConnection(cfg);
        Table table = connection.getTable(TableName.valueOf("test:student"));// 创建Delete对象
        String rowKey = "1003";
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 指定列族删除数据
        // delete.addFamily(Bytes.toBytes(cf));
        // 指定列族:列删除数据(所有版本)
        // delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 指定列族:列删除数据(指定版本)
        // delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));

        // 执行删除数据操作
        table.delete(delete);
        table.close();
        connection.close();

    }
    /**
     * 创建预分区
     */
    @Test
    public void createPartition() {
//        TableDescriptorBuilder td = TableDescriptorBuilder.newBuilder(TableName.valueOf(tn));
//        for ( String cf : cfs ) {
//            final ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder
//                    = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
//            td.setColumnFamily(columnFamilyDescriptorBuilder.build());
//        }
//        byte[][] bss = new byte[2][];
//
//        String r1 = "a";
//        String r2 = "b";
//        bss[0] = Bytes.toBytes(r1);
//        bss[1] = Bytes.toBytes(r2);
//
//        admin.createTable(td.build(),bss);

    }
}
