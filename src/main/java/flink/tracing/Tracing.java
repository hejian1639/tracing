package flink.tracing;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Tracing {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new HttpSource("http://172.17.162.177:8001/trace1.data"))
                .map(line -> new TracingLog(line))
                .filter(tracingLog -> {
                    if (tracingLog.tags.containsKey("http.status_code")) {
                        return tracingLog.tags.getInteger("http.status_code") != 200;

                    }
                    if (tracingLog.tags.containsKey("error")) {
                        return tracingLog.tags.getInteger("error") == 1;

                    }
                    return false;
                })
                .map(tracingLog -> new Tuple2<>(tracingLog.traceId, tracingLog.line))
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .keyBy(0)
                .timeWindowAll(Time.seconds(10))
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1))
                .map(t -> new Tuple2<>(t.f0, MD5Tool.getText(t.f1)))
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .print();


        env.execute("Tracing");
    }


}