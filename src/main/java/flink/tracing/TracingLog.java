package flink.tracing;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
public class TracingLog {
    String traceId;
    String line;
    JSONObject tags;
    long startTime;

    TracingLog(String line) {
        this.line = line;
        String[] result = line.split("\\|");
        traceId = result[0];
        startTime = Long.valueOf(result[1]);
        tags = Tools.getJsonStrByQueryUrl(result[8]);
    }

    @Override
    public String toString() {
        return "TracingLog{" +
                "tags='" + tags + '\'' +
                '}';
    }

}