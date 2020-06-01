package flink.tracing;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Slf4j
public class HttpSource implements SourceFunction<String> {
    String url;
    private volatile boolean isRunning = true;

    public static void main(String[] args) throws Exception {
        try (val client = HttpClientBuilder.create().build()) {
            //发送get请求
            HttpGet request = new HttpGet("http://172.17.162.177:8001/trace1.data");
            HttpResponse response = client.execute(request);

            /**请求发送成功，并得到响应**/
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                /**读取服务器返回过来的json字符串数据**/
                val entity = response.getEntity();

                log.info("entity {}", entity);

                try (val input = entity.getContent()) {
                    log.info("entity {}", input);

                }
            }

        }

    }

    HttpSource(String url) {
        this.url = url;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        try (val client = HttpClientBuilder.create().build()) {
            //发送get请求
            HttpGet request = new HttpGet(url);
            HttpResponse response = client.execute(request);

            /**请求发送成功，并得到响应**/
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                /**读取服务器返回过来的json字符串数据**/
                val entity = response.getEntity();

                log.info("entity {}", entity);

                try (val input = entity.getContent()) {
                    log.info("input {}", input);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                    while (isRunning) {
                        String str = reader.readLine();
                        if (str != null) {
                            ctx.collect(str);

                        } else
                            break;
                    }

                }
            }

        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

