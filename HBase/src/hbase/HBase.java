/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.NavigableMap;
/**
 *
 * @author santaris
 */
public static class HBase {
    private static Configuration conf;
    private static HTable table;
    /**
     * @param args the command line arguments
     */
    public static Create_Table(String name) {
        conf = HBaseConfiguration.create();
        table = new HTable(conf, name);
        
    }
    
    
    public static addData(String data) {
        Get get = new Get(Bytes.toBytes("row1"));
        get.setMaxVersions(3);
        get.addFamily(Bytes.toBytes("colfam1"));
        get.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"));
        // Don't get data unless you ask for it!
//        get.addFamily(Bytes.toBytes("colfam3"));
        Result result = table.get(get);
        String row = Bytes.toString(result.getRow());
    }
    
    
    public static getData(String row, Result result) {
        // Get a specific value
        String specificValue = Bytes.toString(result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")));
        System.out.println("latest colfam1:qual1 is: " + specificValue);

        // Traverse entire returned row
        System.out.println(row);
        NavigableMap<byte[], NavigableMap<byte[],NavigableMap<Long,byte[]>>> map = result.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMapEntry : map.entrySet()) {
            String family = Bytes.toString(navigableMapEntry.getKey());
            System.out.println("\t" + family);
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyContents = navigableMapEntry.getValue();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> mapEntry : familyContents.entrySet()) {
                String qualifier = Bytes.toString(mapEntry.getKey());
                System.out.println("\t\t" + qualifier);
                NavigableMap<Long, byte[]> qualifierContents = mapEntry.getValue();
                for (Map.Entry<Long, byte[]> entry : qualifierContents.entrySet()) {
                    Long timestamp = entry.getKey();
                    String value = Bytes.toString(entry.getValue());
                    System.out.printf("\t\t\t%s, %d\n", value, timestamp);
                }
            }
        }
        table.close();
    }
}
