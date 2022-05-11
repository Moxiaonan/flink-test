package com.xy.flink.test.timer.process;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.xy.flink.test.timer.common.SourceBinlog;
import com.xy.flink.test.timer.common.TestOrder;
import com.xy.flink.test.timer.common.TestOrderDeserializationSchema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Objects;

/**
 * 基于事件时间 , 需要有新的数据推动 watermark 才能看到超时事件 (不一定是同一个key)
 */
public class CdcEventTimeProcess {

    public static void main(String[] args) throws Exception {

        // 获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<SourceBinlog> sourceFunction = MySqlSource.<SourceBinlog>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("mt")
                .tableList("mt.test_order_tab")
                .deserializer(new TestOrderDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .serverId(1)
                .build();
        KeyedStream<SourceBinlog, String> keyedStream;
        keyedStream = env.addSource(sourceFunction).returns(SourceBinlog.class)
            .assignTimestampsAndWatermarks(WatermarkStrategy.<SourceBinlog>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((SerializableTimestampAssigner<SourceBinlog>) (sourceBinlog, l) -> {
                Long mtime = sourceBinlog.getAfter().getMtime();
                return mtime * 1000;
            }))
            .keyBy(source -> {
                String order_no = source.getAfter().getOrder_no();
                return order_no;
            });

        keyedStream.process(new OrderTimeoutProcess()).print("out");

        // 启动任务
        env.execute("FlinkCDC");

    }

    public static class OrderTimeoutProcess extends KeyedProcessFunction<String,SourceBinlog,String>{
        private ValueState<TestOrder> orderCreateState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderCreateState = getRuntimeContext().getState(new ValueStateDescriptor<TestOrder>("order-create",TestOrder.class));
        }

        @Override
        public void processElement(SourceBinlog sourceBinlog, KeyedProcessFunction<String, SourceBinlog, String>.Context context, Collector<String> collector) throws Exception {
            TestOrder testOrder = sourceBinlog.getAfter();
            Long timestamp = context.timestamp();
            if ("CREATE".equals(sourceBinlog.getOp())) {
                orderCreateState.update(testOrder);
                context.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
                collector.collect("订单 : " + testOrder.getOrder_no() + " 创建 , 时间 : " + timestamp);
            }
            if ("UPDATE".equals(sourceBinlog.getOp()) && Objects.equals(testOrder.getOrder_status(),2) && orderCreateState.value() != null) {
                orderCreateState.clear();
                collector.collect("订单 : " + testOrder.getOrder_no() + " 按时完成, 时间 : " + timestamp);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SourceBinlog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            if (orderCreateState != null && orderCreateState.value() != null) {
                TestOrder testOrder = orderCreateState.value();
                orderCreateState.clear();
                out.collect("订单 : " + testOrder.getOrder_no() + " 已超时, 时间 : " + timestamp);
            }
        }
    }
}
