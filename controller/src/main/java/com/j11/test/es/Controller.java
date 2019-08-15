package com.j11.test.es;

import com.alibaba.fastjson.JSON;
import com.j11.es.dto.RiderLocationTimeSerialDTO;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * <pre>
 * <b>Description</b>
 * </pre>
 * <pre>
 * 创建时间 2019-08-15 17:21
 * 所属工程： esrollover  </pre>
 *
 * @author sheldon yhid: 80752866
 */
public class Controller {
    private static TransportClient transportClient;

    static {
        Settings settings = Settings.builder()
                .put("cluster.name", "YourClusterName")
                .build();

        transportClient = new PreBuiltTransportClient(settings);
        // 解析application.properties配置的ES连接地址, 例如 10.19.180.200:9300,10.19.180.192:9300
        String[] nodes = "10.19.180.200:9300,10.19.180.192:9300".split(",");
        for (String node : nodes) {
            // 冒号分割ip和端口号
            String[] hostport = node.split(":");
            String host = hostport[0];
            String port = hostport[1];
            try {
                transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host), Integer.valueOf(port)));
            } catch (UnknownHostException ignored) {
            }
        }
    }

    public void save(String string) {
        RiderLocationTimeSerialDTO dto = JSON.parseObject(string, RiderLocationTimeSerialDTO.class);
        // 1. 检查模板是否存在（第一次需要创建），并滚动
        preIndexCreate();
        // 2. 写入es
        writeToEs(dto);
    }

    private void preIndexCreate() {

    }

    private void writeToEs(RiderLocationTimeSerialDTO dto) {
    }
}
