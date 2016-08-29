package com.htjc.flume.sink.elasticsearch;

import com.alibaba.fastjson.JSON;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by Administrator on 2016/8/16.
 */
public class ElasticSearchMonitorEventSerializer implements ElasticSearchEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchMonitorEventSerializer.class);

    @Override
    public BytesStream getContentBuilder(Event event) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        appendBody(builder, event);
        appendHeaders(builder, event);
        return builder;
    }

    private void appendHeaders(XContentBuilder builder, Event event) {
    }

    private void appendBody(XContentBuilder builder, Event event) throws IOException {
        byte[] data = event.getBody();

        XContentType contentType = XContentFactory.xContentType(data);

        if (contentType != null && contentType.name().equalsIgnoreCase("JSON")) {
            String body = new String(event.getBody(), charset);
            logger.info(body);
            Map<String, Object> map = JSON.parseObject(body, Map.class);

            Set<Map.Entry<String, Object>> set = map.entrySet();
            for (Map.Entry<String, Object> entry : set) {
                String key = entry.getKey();
                Object value = entry.getValue();
                builder.field(key, value);
            }
        } else {
            throw new IOException("数据类型异常:" + contentType);
        }
    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void configure(ComponentConfiguration componentConfiguration) {

    }
}
