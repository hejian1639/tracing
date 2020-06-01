package flink.tracing;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import lombok.val;

public class Tools {
    public static JSONObject getJsonStrByQueryUrl(String paramStr) {
        String[] params = paramStr.split("&");
        JSONObject obj = new JSONObject();
        for (int i = 0; i < params.length; i++) {
            String[] param = params[i].split("=");
            if (param.length >= 2) {
                String key = param[0];
                String value = param[1];
                for (int j = 2; j < param.length; j++) {
                    value += "=" + param[j];
                }
                try {
                    obj.put(key, value);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
        return obj;
    }

    public static void main(String[] args) {
        val json = getJsonStrByQueryUrl("http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET");
        System.out.println(json);
    }
}
