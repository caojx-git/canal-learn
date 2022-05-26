package caojx.learn.tcp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * TCP模式抓取canal数据测试类
 *
 * @author caojx created on 2022/5/26 10:55 AM
 */
public class CanalClient {

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
        //1.获取 canal 连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("localhost", 11111), "example", "", "");

        while (true) {
            // 2.连接
            canalConnector.connect();

            // 3.订阅数据库
            canalConnector.subscribe("gmall.*");

            // 4.获取Message数据
            Message message = canalConnector.get(100);

            // 5.获取Entry集合
            List<CanalEntry.Entry> entries = message.getEntries();

            // 6.判断集合是否为空如果为空则等待一会继续拉取数据
            if (entries.size() <= 0) {
                System.out.println("没有数据，休息一会");
                Thread.sleep(1000);
            } else {
                // 遍历entries，单条机械
                for (CanalEntry.Entry entry : entries) {
                    // 获取表名
                    String tableName = entry.getHeader().getTableName();

                    // 获取类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    // 获取序列化后的数据
                    ByteString storeValue = entry.getStoreValue();

                    // 判断当前entryType是否为ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        // 反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        // 获取当前事件的操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        // 获取数据集
                        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

                        // 遍历rowDataList，并打印数据
                        for (CanalEntry.RowData rowData : rowDataList) {
                            JSONObject beforeData = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                beforeData.put(column.getName(), column.getValue());
                            }

                            JSONObject afterData = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            for (CanalEntry.Column column : afterColumnsList) {
                                afterData.put(column.getName(), column.getValue());
                            }

                            System.out.println("TableName:" + tableName
                                    + ",EventType:" + eventType +
                                    ",After:" + beforeData +
                                    ",After:" + afterData);
                        }

                    } else {
                        System.out.println("当期操作类型为：" + entryType);
                    }
                }
            }
        }
    }
}
