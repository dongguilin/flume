/**
 * Copyright 2006-2014 handu.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.htjc.flume.source.rocketmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.consumer.MQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RocketMQSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);

    private String topic;
    private String tags;
    private String topicHeaderName;
    private String tagsHeaderName;
    private int maxNums;
    private MQPullConsumer consumer;

    @Override
    public void configure(Context context) {
        // 初始化配置项
        topic = Preconditions.checkNotNull(context.getString(RocketMQSourceUtil.TOPIC_CONFIG), "RocketMQ topic must be specified. For example: a1.sources.r1.topic=TestTopic");
        tags = context.getString(RocketMQSourceUtil.TAGS_CONFIG, RocketMQSourceUtil.TAGS_DEFAULT);
        topicHeaderName = context.getString(RocketMQSourceUtil.TOPIC_HEADER_NAME_CONFIG, RocketMQSourceUtil.TOPIC_HEADER_NAME_DEFAULT);
        tagsHeaderName = context.getString(RocketMQSourceUtil.TAGS_HEADER_NAME_CONFIG, RocketMQSourceUtil.TAGS_HEADER_NAME_DEFAULT);
        maxNums = context.getInteger(RocketMQSourceUtil.MAXNUMS_CONFIG, RocketMQSourceUtil.MAXNUMS_DEFAULT);

        // 初始化Consumer
        consumer = Preconditions.checkNotNull(RocketMQSourceUtil.getConsumer(context));
        // 必须加上此监听才能在消费过后，自动回写消费进度
        consumer.registerMessageQueueListener(topic, null);
    }

    @Override
    public Status process() throws EventDeliveryException {
        List<Event> eventList = Lists.newArrayList();
        Map<MessageQueue, Long> offsetMap = Maps.newHashMap();

        try {
            Set<MessageQueue> mqs = Preconditions.checkNotNull(consumer.fetchSubscribeMessageQueues(topic));
            for (MessageQueue mq : mqs) {
                // 获取offset
                long offset = consumer.fetchConsumeOffset(mq, false);
                offset = offset < 0 ? 0 : offset;
                PullResult pullResult = consumer.pull(mq, tags, offset, maxNums);
                // 发现新消息写入Event
                if (pullResult.getPullStatus() == PullStatus.FOUND) {
                    for (MessageExt messageExt : pullResult.getMsgFoundList()) {
                        Event event = new SimpleEvent();
                        Map<String, String> headers = new HashMap<String, String>();
                        headers.put(topicHeaderName, messageExt.getTopic());
                        headers.put(tagsHeaderName, messageExt.getTags());

                        //业务时间timestamp
                        String body = new String(messageExt.getBody(), Charset.defaultCharset());
                        Map<String, Object> map = JSON.parseObject(body, Map.class);
                        if (map.containsKey("timestamp")) {
                            headers.put("timestamp", map.get("timestamp").toString());
                        } else {
                            headers.put("timestamp", System.currentTimeMillis() + "");
                        }
                        map.remove("timestamp");

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("MessageQueue={}, Topic={}, Tags={}, Message: {}", new Object[]{
                                    mq, messageExt.getTopic(), messageExt.getTags(), messageExt.getBody()});
                        }
                        event.setBody(messageExt.getBody());
                        event.setHeaders(headers);
                        eventList.add(event);
                    }
                    offsetMap.put(mq, pullResult.getNextBeginOffset());
                }
            }

            if (CollectionUtils.isNotEmpty(eventList)) {
                // 批量处理事件
                getChannelProcessor().processEventBatch(eventList);
                for (Map.Entry<MessageQueue, Long> entry : offsetMap.entrySet()) {
                    // 更新offset
                    consumer.updateConsumeOffset(entry.getKey(), entry.getValue());
                }
            }

        } catch (Exception e) {
            LOG.error("RocketMQSource consume message exception", e);
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    public synchronized void start() {
        try {
            // 启动Consumer
            consumer.start();
        } catch (MQClientException e) {
            LOG.error("RocketMQSource start consumer failed", e);
            Throwables.propagate(e);
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        // 停止Consumer
        consumer.shutdown();
        super.stop();
    }

}
