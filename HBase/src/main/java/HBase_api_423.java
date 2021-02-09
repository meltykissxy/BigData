import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBase_api_423 {
    public static void main(String[] args) throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        Connection connection = ConnectionFactory.createConnection(cfg);
        final Admin admin = connection.getAdmin();

        String tn = "test:emp"; //命名空间:表名
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
}
