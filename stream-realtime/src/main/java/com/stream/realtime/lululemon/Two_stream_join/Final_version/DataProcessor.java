package com.stream.realtime.lululemon.Two_stream_join.Final_version;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.alibaba.fastjson2.JSONObject;

import java.util.List;

/**
 * 数据处理类 - 包含数据解析和流处理
 */
public class DataProcessor {

    // === 数据解析方法 ===

    public static DataStream<DataModel.CommentData> parseCommentStream(DataStream<String> commentStream) {
        return commentStream
            .map(new CommentParser())
            .filter(comment -> comment != null)
            .name("parsed-comment-stream");
    }

    public static DataStream<DataModel.LogData> parseLogStream(DataStream<String> logsStream) {
        return logsStream
            .map(new LogParser())
            .filter(log -> log != null)
            .name("parsed-log-stream");
    }

    public static DataStream<DataModel.UserInfoData> parseUserInfoStream(DataStream<String> userInfoStream) {
        return userInfoStream
            .map(new UserInfoParser())
            .filter(userInfo -> userInfo != null)
            .name("parsed-userinfo-stream");
    }

    // === 数据解析器 ===

    public static class CommentParser implements MapFunction<String, DataModel.CommentData> {
        @Override
        public DataModel.CommentData map(String json) throws Exception {
            try {
                JSONObject jsonObject = JSONObject.parseObject(json);
                String orderId = jsonObject.getString("order_id");
                String userId = jsonObject.getString("user_id");
                String sensitiveLevel = jsonObject.getString("sensitive_level");
                Boolean isBlocked = jsonObject.getBoolean("is_blocked");
                Integer banDays = jsonObject.getInteger("blacklist_duration_days");
                String triggeredKeyword = jsonObject.getString("triggered_keyword");
                Double totalAmount = jsonObject.getDouble("total_amount");
                String consumptionLevel = jsonObject.getString("consumption_level");
                String userComment = jsonObject.getString("user_comment");
                String ds = jsonObject.getString("ds");
                Long ts = jsonObject.getLong("ts");
                String productId = jsonObject.getString("product_id");

                System.out.println("🔵 解析 Comment - UserID: " + userId + ", OrderID: " + orderId + ", 时间: " + ts);
                return new DataModel.CommentData(orderId, userId, sensitiveLevel, isBlocked, banDays,
                        triggeredKeyword, totalAmount, consumptionLevel, userComment, ds, ts, productId);
            } catch (Exception e) {
                System.err.println("❌ 解析 Comment 数据失败: " + json);
                return null;
            }
        }
    }

    public static class LogParser implements MapFunction<String, DataModel.LogData> {
        @Override
        public DataModel.LogData map(String json) throws Exception {
            try {
                System.out.println("🟢 开始解析 Log 数据: " + json);

                JSONObject jsonObject = JSONObject.parseObject(json);
                String orderId = jsonObject.getString("order_id");
                String userId = jsonObject.getString("user_id");
                String productId = jsonObject.getString("product_id");
                String logId = jsonObject.getString("log_id");
                String logType = jsonObject.getString("log_type");

                // 处理timestamp
                Long ts;
                Object tsObj = jsonObject.get("ts");
                if (tsObj instanceof Double) {
                    ts = ((Double) tsObj).longValue();
                } else if (tsObj instanceof Float) {
                    ts = ((Float) tsObj).longValue();
                } else {
                    ts = jsonObject.getLong("ts");
                }

                // 解析IP地址
                String ipAddress = null;
                if (jsonObject.containsKey("gis")) {
                    JSONObject gis = jsonObject.getJSONObject("gis");
                    if (gis != null && gis.containsKey("ip")) {
                        ipAddress = gis.getString("ip");
                    }
                }

                // 解析设备信息
                DataModel.DeviceInfo deviceInfo = null;
                if (jsonObject.containsKey("device")) {
                    JSONObject deviceObj = jsonObject.getJSONObject("device");
                    String brand = deviceObj.getString("brand");
                    String plat = deviceObj.getString("plat");
                    String platv = deviceObj.getString("platv");
                    String softv = deviceObj.getString("softv");
                    String device = deviceObj.getString("device");
                    String userkey = deviceObj.getString("userkey");

                    deviceInfo = new DataModel.DeviceInfo(brand, plat, platv, softv, device, userkey);
                    System.out.println("📱 解析设备信息 - 品牌: " + brand + ", 平台: " + plat + ", 设备: " + device);
                }

                // 解析 keywords 字段 - 新增
                List<String> keywords = null;
                if (jsonObject.containsKey("keywords") && "search".equals(logType)) {
                    keywords = jsonObject.getJSONArray("keywords").toList(String.class);
                    System.out.println("🔍 搜索关键词: " + keywords);
                }

                System.out.println("🟢 解析 Log 成功 - UserID: " + userId +
                        ", OrderID: " + orderId + ", 日志类型: " + logType + ", 时间: " + ts + ", IP: " + ipAddress);
                return new DataModel.LogData(orderId, userId, productId, logId, ts, ipAddress, deviceInfo, logType, keywords);
            } catch (Exception e) {
                System.err.println("❌ 解析 Log 数据失败: " + json);
                e.printStackTrace();
                return null;
            }
        }
    }
    public static class UserInfoParser implements MapFunction<String, DataModel.UserInfoData> {
        @Override
        public DataModel.UserInfoData map(String json) throws Exception {
            try {
                System.out.println("🟣 开始解析 UserInfo 数据: " + json);

                JSONObject jsonObject = JSONObject.parseObject(json);

                // 解析嵌套的 data 数组
                JSONObject data = jsonObject.getJSONArray("data").getJSONObject(0);
                String userId = data.getString("user_id");
                String uname = data.getString("uname");
                Integer gender = data.getInteger("gender");
                String ageGroup = data.getString("age_group");
                String constellation = data.getString("constellation");
                String birthday = data.getString("birthday");
                Long ts = jsonObject.getLong("ts");

                System.out.println("🟣 解析 UserInfo 成功 - UserID: " + userId +
                        ", 姓名: " + uname + ", 性别: " + gender + ", 年龄组: " + ageGroup);
                return new DataModel.UserInfoData(userId, uname, gender, ageGroup, constellation, birthday, ts);
            } catch (Exception e) {
                System.err.println("❌ 解析 UserInfo 数据失败: " + json);
                e.printStackTrace();
                return null;
            }
        }
    }


}