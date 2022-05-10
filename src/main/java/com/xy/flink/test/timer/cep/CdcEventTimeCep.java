package com.xy.flink.test.timer.cep;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.xy.flink.test.timer.common.SourceBinlog;
import com.xy.flink.test.timer.common.TestOrderDeserializationSchema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipPastLastStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CdcEventTimeCep {

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
        KeyedStream<SourceBinlog, String> keyedStream =
                env.addSource(sourceFunction).returns(SourceBinlog.class)
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<SourceBinlog>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((SerializableTimestampAssigner<SourceBinlog>) (sourceBinlog, l) -> {
                        Long mtime = sourceBinlog.getAfter().getMtime();
                        return mtime * 1000;
                    }))
                    .keyBy(source -> {
                        String order_no = source.getAfter().getOrder_no();
                        return order_no;
                    });

        keyedStream.print();

        // 模式定义
        Pattern<SourceBinlog, ?> pattern = Pattern
                .<SourceBinlog>begin("order_create")
                .where(new IterativeCondition<SourceBinlog>() {
                    @Override
                    public boolean filter(SourceBinlog sourceBinlog, Context<SourceBinlog> context) throws Exception {
                        String op = sourceBinlog.getOp();
                        boolean opFlag = "CREATE".equals(op);
                        return opFlag;
                    }
                })
                .followedBy("order_end")
                .where(new IterativeCondition<SourceBinlog>() {
                    @Override
                    public boolean filter(SourceBinlog sourceBinlog, Context<SourceBinlog> context) throws Exception {
                        boolean updateFlag = "UPDATE".equals(sourceBinlog.getOp());
                        boolean endFlag = false;
                        if (sourceBinlog.getAfter() != null) {
                            endFlag = sourceBinlog.getAfter().getOrder_status().equals(2);
                        }
                        return updateFlag && endFlag;
                    }
                })
                .within(Time.seconds(10));

        // 开始匹配
        PatternStream<SourceBinlog> patternStream = CEP.pattern(keyedStream, pattern);

        // 匹配处理
        SingleOutputStreamOperator<String> patternProcessStream = patternStream.process(new OrderEndPatternProcessFunction());

        OutputTag<String> lateDataOutputTag = new OutputTag<String>("outOfTime"){};
        patternProcessStream.print("finish");
        patternProcessStream.getSideOutput(lateDataOutputTag).print("!!! out of time");


        // 启动任务
        env.execute("FlinkCDC");

    }

    public static class OrderEndPatternProcessFunction extends PatternProcessFunction<SourceBinlog, String> implements TimedOutPartialMatchHandler<SourceBinlog> {

        @Override
        public void processMatch(Map<String, List<SourceBinlog>> map, Context context, Collector<String> collector) throws Exception {
            List<SourceBinlog> order_create = map.get("order_create");
            List<SourceBinlog> order_end = map.get("order_end");
            Long afterTime = System.currentTimeMillis();
            if (order_end != null && order_end.size() > 0) {
                afterTime = order_end.get(0).getAfter().getMtime();
            }

            collector.collect(
                "order:" + order_create.get(0).getAfter().getOrder_no() + " end in " + afterTime
            );
        }

        @Override
        public void processTimedOutMatch(Map<String, List<SourceBinlog>> map, Context context) throws Exception {
            List<SourceBinlog> order_create = map.get("order_create");
            context.output(new OutputTag<String>("outOfTime"){},"order: " + order_create.get(0).getAfter().getOrder_no() + " out of time");
        }
    }

}
