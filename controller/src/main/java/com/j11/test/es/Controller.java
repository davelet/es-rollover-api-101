package com.j11.test.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.j11.es.dto.RiderLocationTimeSerialDTO;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
@Slf4j
public class Controller {
    private static TransportClient transportClient;
    private final static String yourTemplateName = "YourTemplateName";
    private final static String yourWriteAlias = "yourWriteAlias";
    private final static String YourReadAlias = "YourReadAlias";

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
        GetIndexTemplatesRequest gitr = new GetIndexTemplatesRequest(yourTemplateName);
        try {
            GetIndexTemplatesResponse response = transportClient.admin().indices().getTemplates(gitr).get(100, TimeUnit.MILLISECONDS);
            List<IndexTemplateMetaData> list = response.getIndexTemplates();
            if (!list.isEmpty()) {
                // 模板存在 滚动即可
                RolloverRequest rr = new RolloverRequest(yourWriteAlias, null);
                rr.addMaxIndexAgeCondition(new TimeValue(10, TimeUnit.DAYS));
                rr.addMaxIndexDocsCondition(1_0000_0000);
                ActionFuture<RolloverResponse> index = transportClient.admin().indices().rolloversIndex(rr);
                return;
            }
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            log.warn("error", e);
            return;
        }

        // 模板不存在 先创建模板
        PutIndexTemplateRequest pitr = new PutIndexTemplateRequest()
                .name(yourTemplateName)
                .patterns(Collections.singletonList("my-test-index-*"))
                .alias(new Alias(YourReadAlias));
        try {
            PutIndexTemplateResponse response = transportClient.admin().indices().putTemplate(pitr).get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("error", e);
        }

        // 创建索引
        CreateIndexRequest cir = new CreateIndexRequest()
                .index("my-test-index-1")
                .alias(new Alias(yourWriteAlias));
        try {
            CreateIndexResponse response = transportClient.admin().indices().create(cir).get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("error", e);
        }
    }

    private void writeToEs(RiderLocationTimeSerialDTO o) {
        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
        // rollover应用的场景一般是写入频繁的，这里虽然演示的只有一个对象，但是实际应该把对象缓存起来，够一定数量了调用bulk api
        List<RiderLocationTimeSerialDTO> list = Collections.singletonList(o);

        for (RiderLocationTimeSerialDTO dto : list) {
            Map<String, Object> map = JSON.parseObject(JSON.toJSONString(dto), new TypeReference<Map<String, Object>>() {
            });
            map.remove("timestamp");
            UpdateRequestBuilder updateRequestBuilder = transportClient.prepareUpdate(yourWriteAlias, "type", "自定义Id" + System.currentTimeMillis());
            updateRequestBuilder.setDoc(map).setUpsert(map);
            updateRequestBuilder.setRetryOnConflict(1);
            bulkRequest.add(updateRequestBuilder);
        }
        if (bulkRequest.request().numberOfActions() == 0) {
            return;
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse br : bulkResponse.getItems()) {
                log.warn("save failed: {}", br.getId());
            }
        }
    }
}
