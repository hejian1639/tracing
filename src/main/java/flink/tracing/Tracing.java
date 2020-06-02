package flink.tracing;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Tracing {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        val source1 = env.addSource(new HttpSource("http://172.17.162.177:8001/trace1.data"));
        val source2 = env.addSource(new HttpSource("http://172.17.162.177:8001/trace2.data"));

        source1.connect(source2)
                .map(new CoMapFunction<String, String, TracingLog>() {
                    @Override
                    public TracingLog map1(String line) {
                        return new TracingLog(line);
                    }

                    @Override
                    public TracingLog map2(String line) {
                        return new TracingLog(line);
                    }
                })
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
//                .countWindow(2)
                .window(GlobalWindows.create()).trigger(LogCountTrigger.of(20000))
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1))
                .map(t -> new Tuple2<>(t.f0, MD5Tool.getText(t.f1)))
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .addSink(new OutputSink()).name("Output");


        env.execute("Tracing");
    }

    static class OutputSink implements SinkFunction<Tuple2<String, String>> {
        Map<String, String> map = new HashMap<>();

        @Override
        public void invoke(Tuple2<String, String> record) {
            System.out.println(record);
            map.put(record.f0, record.f1);
        }

    }
}