package com.xy.flink.test.timer.cep;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.xy.flink.test.timer.common.SourceBinlog;
import com.xy.flink.test.timer.common.TestOrderDeserializationSchema;
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

import java.util.List;
import java.util.Map;

/**
 * 1. 基于处理时间 , 可以实时输出正常结束的处理流的结果 , 不需要新的事件推动 watermark ; 处理超时流时 , 可以马上输出
 * 2. 处理事件下 , CEP 必须等待 pattern 匹配事件全部到达 , 才判断是否超时 , 而不是超时的时候 , 去判断是否匹配
 * 如 : 新增订单1 , 并 10s 内结束 , 马上输出 '订单结束';
 *     新增订单1 , 并 10s 内没有结束 , 不会马上输出 '超时' , 需要等待这个订单1的状态更新事件导到 , 才会输出 '超时' , 新增订单2事件到达 , 也不会输出 '超时' ;
 */
public class CdcProcessTimeCep {

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
                    .keyBy(source -> {
                        String order_no = source.getAfter().getOrder_no();
                        return order_no;
                    });

        keyedStream.print();

        // 模式定义
        SkipPastLastStrategy skipPastLastStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<SourceBinlog, ?> pattern = Pattern
//                .<SourceBinlog>begin("order_create")
                .<SourceBinlog>begin("order_create",skipPastLastStrategy)
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
        PatternStream<SourceBinlog> patternStream = CEP.pattern(keyedStream, pattern).inProcessingTime();

        // 匹配处理
        SingleOutputStreamOperator<String> patternProcessStream = patternStream.process(new OrderEndPatternProcessFunction());

        OutputTag<String> lateDataOutputTag = new OutputTag<String>("outOfTime"){};
        patternProcessStream.print("finish");
        patternProcessStream.getSideOutput(lateDataOutputTag).print("!!!");


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
                "订单 : " + order_create.get(0).getAfter().getOrder_no() + " 按时完成, 时间 : " + afterTime
            );
        }

        @Override
        public void processTimedOutMatch(Map<String, List<SourceBinlog>> map, Context context) throws Exception {
            List<SourceBinlog> order_create = map.get("order_create");
            context.output(new OutputTag<String>("outOfTime"){},"订单 : " + order_create.get(0).getAfter().getOrder_no() + "  已超时");
        }
    }

}
