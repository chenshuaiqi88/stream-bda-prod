package com.stream.realtime.lululemon.Two_stream_join.First_version;

import com.stream.core.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson2.JSONObject;

import java.io.IOException;
import java.util.*;

/**
 * 三流 CoProcessFunction Join 处理器 - 基于 UserID 连接，Log中设备信息去重
 *
 */
public class KFKlog_KFKcomment {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取 comment-security-results 主题
        KafkaSource<String> commentSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092",
                "comment-security-results",
                "flink-comment-reader",
                OffsetsInitializer.earliest()
        );

        // 读取 realtime_v3_logs 主题
        KafkaSource<String> logsSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092",
                "realtime_v3_logs",
                "flink-logs-reader",
                OffsetsInitializer.earliest()
        );

        // 读取 user_info_enhanced 主题
        KafkaSource<String> userInfoSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092",
                "user_info_enhanced",
                "flink-userinfo-reader",
                OffsetsInitializer.earliest()
        );

        // 处理 comment-security-results 数据
        DataStream<String> commentStream = env.fromSource(
                commentSource,
                WatermarkStrategy.noWatermarks(),
                "comment-security-source"
        );

        // 处理 realtime_v3_logs 数据
        DataStream<String> logsStream = env.fromSource(
                logsSource,
                WatermarkStrategy.noWatermarks(),
                "realtime-logs-source"
        );

        // 处理 user_info_enhanced 数据
        DataStream<String> userInfoStream = env.fromSource(
                userInfoSource,
                WatermarkStrategy.noWatermarks(),
                "user-info-source"
        );

        // 输出原始数据用于调试
        commentStream
                .map(record -> {
                    System.out.println("📥 [RAW-COMMENT] " + record);  // 修改前缀
                    return record;
                })
                .name("comment-processor");

        logsStream
                .map(record -> {
                    System.out.println("🟢 [realtime_v3_logs] " + record);
                    return record;
                })
                .name("logs-processor");

        userInfoStream
                .map(record -> {
                    System.out.println("🟣 [user_info_enhanced] " + record);
                    return record;
                })
                .name("user-info-processor");

        // 解析数据流
        DataStream<CommentData> parsedCommentStream = commentStream
                .map(new CommentParser())
                .filter(comment -> comment != null)
                .name("parsed-comment-stream");

        DataStream<LogData> parsedLogStream = logsStream
                .map(new LogParser())
                .filter(log -> log != null)
                .name("parsed-log-stream");

        DataStream<UserInfoData> parsedUserInfoStream = userInfoStream
                .map(new UserInfoParser())
                .filter(userInfo -> userInfo != null)
                .name("parsed-userinfo-stream");

        // 第一步：Comment 和 Log 连接
        SingleOutputStreamOperator<CommentLogJoinResult> commentLogJoinedStream = parsedCommentStream
                .keyBy(comment -> comment.userId)
                .connect(parsedLogStream.keyBy(log -> log.userId))
                .process(new CommentLogCoProcessFunction())
                .name("comment-log-join");

        // 第二步：将 Comment-Log 连接结果与 UserInfo 连接
        SingleOutputStreamOperator<FinalJoinResult> finalJoinedStream = commentLogJoinedStream
                .keyBy(result -> result.userId)
                .connect(parsedUserInfoStream.keyBy(userInfo -> userInfo.userId))
                .process(new CommentLogUserInfoCoProcessFunction())
                .name("final-join");

        // 输出最终 Join 结果
        finalJoinedStream
                .map(result -> {
                    String json = result.toJson();
                    System.out.println("🎯 ===== 最终 JOIN 结果 =====");
                    System.out.println("🎯 " + json);
                    System.out.println("🎯 =====================");
                    return json;
                })
                .name("final-join-result-printer");

        env.execute("Three Stream Join with CoProcessFunction - UserID Join with Device Deduplication in Log");
    }


    /**
     * 设备信息模型 - 根据brand和plat分组去重
     */
    public static class DeviceInfo {
        public String brand;
        public String plat;
        public String platv;
        public String softv;
        public String device;
        public String userkey;

        public DeviceInfo(String brand, String plat, String platv, String softv, String device, String userkey) {
            this.brand = brand;
            this.plat = plat;
            this.platv = platv;
            this.softv = softv;
            this.device = device;
            this.userkey = userkey;
        }

        /**
         * 获取分组键 - 用于根据brand和plat分组
         */
        public String getGroupKey() {
            return (brand + "_" + plat).toLowerCase();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DeviceInfo that = (DeviceInfo) obj;
            // 根据brand和plat判断相等性
            return brand.equalsIgnoreCase(that.brand) &&
                    plat.equalsIgnoreCase(that.plat);
        }

        @Override
        public int hashCode() {
            // 基于brand和plat生成hashCode
            return (brand.toLowerCase() + "_" + plat.toLowerCase()).hashCode();
        }

        /**
         * 完整信息相等性比较（用于严格比较）
         */
        public boolean fullEquals(DeviceInfo that) {
            if (this == that) return true;
            if (that == null) return false;
            return brand.equals(that.brand) &&
                    plat.equals(that.plat) &&
                    platv.equals(that.platv) &&
                    softv.equals(that.softv) &&
                    device.equals(that.device) &&
                    userkey.equals(that.userkey);
        }
    }

    /**
     * Comment 数据模型
     */
    public static class CommentData {
        public String orderId;
        public String userId;
        public String sensitiveLevel;
        public Boolean isBlocked;
        public Integer banDays;
        public String triggeredKeyword;
        public Double totalAmount;
        public String consumptionLevel;
        public String userComment;
        public String ds;
        public Long ts;
        public String productId;

        public CommentData(String orderId, String userId, String sensitiveLevel, Boolean isBlocked,
                           Integer banDays, String triggeredKeyword, Double totalAmount,
                           String consumptionLevel, String userComment, String ds, Long ts, String productId) {
            this.orderId = orderId;
            this.userId = userId;
            this.sensitiveLevel = sensitiveLevel;
            this.isBlocked = isBlocked;
            this.banDays = banDays;
            this.triggeredKeyword = triggeredKeyword;
            this.totalAmount = totalAmount;
            this.consumptionLevel = consumptionLevel;
            this.userComment = userComment;
            this.ds = ds;
            this.ts = ts;
            this.productId = productId;
        }
    }

    /**
     * Log 数据模型 - 添加设备信息
     */
    public static class LogData {
        public String orderId;
        public String userId;
        public String productId;
        public String logId;
        public Long ts;
        public String ipAddress;
        public DeviceInfo deviceInfo;
        public String logType;

        public LogData(String orderId, String userId, String productId, String logId, Long ts, 
                      String ipAddress, DeviceInfo deviceInfo, String logType) {
            this.orderId = orderId;
            this.userId = userId;
            this.productId = productId;
            this.logId = logId;
            this.ts = ts;
            this.ipAddress = ipAddress;
            this.deviceInfo = deviceInfo;
            this.logType = logType;
        }
    }

    /**
     * UserInfo 数据模型
     */
    public static class UserInfoData {
        public String userId;
        public String uname;
        public Integer gender;
        public String ageGroup;
        public String constellation;
        public String birthday;
        public Long ts;

        public UserInfoData(String userId, String uname, Integer gender, String ageGroup, 
                           String constellation, String birthday, Long ts) {
            this.userId = userId;
            this.uname = uname;
            this.gender = gender;
            this.ageGroup = ageGroup;
            this.constellation = constellation;
            this.birthday = birthday;
            this.ts = ts;
        }
    }

    /**
     * 设备分组工具类
     */
    public static class DeviceGroupUtils {

        /**
         * 根据brand和plat分组去重设备集合
         */
        public static Set<DeviceInfo> groupAndDeduplicate(Set<DeviceInfo> devices) {
            if (devices == null || devices.isEmpty()) {
                return new HashSet<>();
            }

            Set<DeviceInfo> groupedDevices = new HashSet<>();
            Set<String> groupKeys = new HashSet<>();

            for (DeviceInfo device : devices) {
                String groupKey = device.getGroupKey();
                if (!groupKeys.contains(groupKey)) {
                    groupedDevices.add(device);
                    groupKeys.add(groupKey);
                    System.out.println("📱 添加分组设备 - 品牌: " + device.brand + ", 平台: " + device.plat);
                } else {
                    System.out.println("⏭️ 跳过重复分组设备 - 品牌: " + device.brand + ", 平台: " + device.plat);
                }
            }

            System.out.println("📊 设备分组完成 - 原始数量: " + devices.size() + ", 分组后: " + groupedDevices.size());
            return groupedDevices;
        }

        /**
         * 获取分组统计信息
         */
        public static void printGroupStats(Set<DeviceInfo> devices) {
            if (devices == null || devices.isEmpty()) {
                System.out.println("📊 无设备信息");
                return;
            }

            Map<String, List<DeviceInfo>> groupMap = new HashMap<>();
            for (DeviceInfo device : devices) {
                String groupKey = device.getGroupKey();
                groupMap.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(device);
            }

            System.out.println("📊 设备分组统计:");
            for (Map.Entry<String, List<DeviceInfo>> entry : groupMap.entrySet()) {
                System.out.println("   📍 " + entry.getKey() + ": " + entry.getValue().size() + " 个设备");
                for (DeviceInfo device : entry.getValue()) {
                    System.out.println("      └─ " + device.device + " (" + device.platv + ")");
                }
            }
        }
    }


    /**
     * Comment-Log 连接结果模型
     */
    public static class CommentLogJoinResult {
        public CommentData comment;
        public LogData log;
        public String userId;
        public Long joinTime;
        public Set<DeviceInfo> devices; // 设备信息集合，自动去重

        public CommentLogJoinResult(CommentData comment, LogData log, Set<DeviceInfo> devices) {
            this.comment = comment;
            this.log = log;
            this.userId = comment != null ? comment.userId : (log != null ? log.userId : null);
            this.joinTime = System.currentTimeMillis();
            this.devices = devices;
        }
    }

    /**
     * 最终连接结果模型 - 设备信息放在log字段内
     */
    public static class FinalJoinResult {
        public CommentData comment;
        public LogData log;
        public UserInfoData userInfo;
        public Long joinTime;
        public Set<DeviceInfo> devices;

        public FinalJoinResult(CommentData comment, LogData log, UserInfoData userInfo, Set<DeviceInfo> devices) {
            this.comment = comment;
            this.log = log;
            this.userInfo = userInfo;
            this.joinTime = System.currentTimeMillis();
            this.devices = devices;
        }

        public String toJson() {
            JSONObject json = new JSONObject();
            json.put("join_time", joinTime);

            if (comment != null) {
                JSONObject commentJson = new JSONObject();
                commentJson.put("order_id", comment.orderId);
                commentJson.put("user_id", comment.userId);
                commentJson.put("sensitive_level", comment.sensitiveLevel);
                commentJson.put("is_blocked", comment.isBlocked);
                commentJson.put("ban_days", comment.banDays);
                commentJson.put("triggered_keyword", comment.triggeredKeyword);
                commentJson.put("total_amount", comment.totalAmount);
                commentJson.put("consumption_level", comment.consumptionLevel);
                commentJson.put("user_comment", comment.userComment);
                commentJson.put("ds", comment.ds);
                commentJson.put("ts", comment.ts);
                commentJson.put("product_id", comment.productId);
                json.put("comment", commentJson);
            }

            // 在log字段内添加devices数组
            JSONObject logJson = new JSONObject();
            if (log != null) {
                logJson.put("order_id", log.orderId);
                logJson.put("user_id", log.userId);
                logJson.put("product_id", log.productId);
                logJson.put("log_id", log.logId);
                logJson.put("ts", log.ts);
                logJson.put("ip_address", log.ipAddress);
                logJson.put("log_type", log.logType);
            }

            // 添加分组后的设备信息数组到log字段内
            if (devices != null && !devices.isEmpty()) {
                List<JSONObject> deviceList = new ArrayList<>();
                for (DeviceInfo device : devices) {
                    JSONObject deviceJson = new JSONObject();
                    deviceJson.put("brand", device.brand);
                    deviceJson.put("plat", device.plat);
                    deviceJson.put("platv", device.platv);
                    deviceJson.put("softv", device.softv);
                    deviceJson.put("device", device.device);
                    deviceJson.put("userkey", device.userkey);
                    deviceJson.put("group_key", device.getGroupKey()); // 添加分组键
                    deviceList.add(deviceJson);
                }
                logJson.put("devices", deviceList);
                System.out.println("📱 用户 " + (userInfo != null ? userInfo.userId : comment != null ? comment.userId : log.userId) +
                        " 使用了 " + devices.size() + " 种分组设备");

                // 打印分组统计
                DeviceGroupUtils.printGroupStats(devices);
            }

            json.put("log", logJson);

            if (userInfo != null) {
                JSONObject userInfoJson = new JSONObject();
                userInfoJson.put("user_id", userInfo.userId);
                userInfoJson.put("uname", userInfo.uname);
                userInfoJson.put("gender", userInfo.gender);
                userInfoJson.put("age_group", userInfo.ageGroup);
                userInfoJson.put("constellation", userInfo.constellation);
                userInfoJson.put("birthday", userInfo.birthday);
                userInfoJson.put("ts", userInfo.ts);
                json.put("user_info", userInfoJson);
            }

            return json.toJSONString();
        }
    }

    /**
     * Comment 数据解析器
     */
    public static class CommentParser implements MapFunction<String, CommentData> {
        @Override
        public CommentData map(String json) throws Exception {
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
                return new CommentData(orderId, userId, sensitiveLevel, isBlocked, banDays,
                        triggeredKeyword, totalAmount, consumptionLevel,
                        userComment, ds, ts, productId);
            } catch (Exception e) {
                System.err.println("❌ 解析 Comment 数据失败: " + json);
                return null;
            }
        }
    }

    /**
     * Log 数据解析器 - 增强版，解析设备信息
     */
    public static class LogParser implements MapFunction<String, LogData> {
        @Override
        public LogData map(String json) throws Exception {
            try {
                System.out.println("🟢 开始解析 Log 数据: " + json);

                JSONObject jsonObject = JSONObject.parseObject(json);
                String orderId = jsonObject.getString("order_id");
                String userId = jsonObject.getString("user_id");
                String productId = jsonObject.getString("product_id");
                String logId = jsonObject.getString("log_id");
                String logType = jsonObject.getString("log_type");

                // 处理带小数点的 timestamp
                Long ts;
                Object tsObj = jsonObject.get("ts");
                if (tsObj instanceof Double) {
                    ts = ((Double) tsObj).longValue();
                } else if (tsObj instanceof Float) {
                    ts = ((Float) tsObj).longValue();
                } else {
                    ts = jsonObject.getLong("ts");
                }

                // 解析嵌套的 IP 地址
                String ipAddress = null;
                if (jsonObject.containsKey("gis")) {
                    JSONObject gis = jsonObject.getJSONObject("gis");
                    if (gis != null && gis.containsKey("ip")) {
                        ipAddress = gis.getString("ip");
                    }
                }

                // 解析设备信息
                DeviceInfo deviceInfo = null;
                if (jsonObject.containsKey("device")) {
                    JSONObject deviceObj = jsonObject.getJSONObject("device");
                    String brand = deviceObj.getString("brand");
                    String plat = deviceObj.getString("plat");
                    String platv = deviceObj.getString("platv");
                    String softv = deviceObj.getString("softv");
                    String device = deviceObj.getString("device");
                    String userkey = deviceObj.getString("userkey");
                    
                    deviceInfo = new DeviceInfo(brand, plat, platv, softv, device, userkey);
                    System.out.println("📱 解析设备信息 - 品牌: " + brand + ", 平台: " + plat + ", 设备: " + device);
                }

                System.out.println("🟢 解析 Log 成功 - UserID: " + userId +
                        ", OrderID: " + orderId +
                        ", 日志类型: " + logType +
                        ", 时间: " + ts +
                        ", IP: " + ipAddress);
                return new LogData(orderId, userId, productId, logId, ts, ipAddress, deviceInfo, logType);
            } catch (Exception e) {
                System.err.println("❌ 解析 Log 数据失败: " + json);
                System.err.println("❌ 错误信息: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
    }

    /**
     * UserInfo 数据解析器
     */
    public static class UserInfoParser implements MapFunction<String, UserInfoData> {
        @Override
        public UserInfoData map(String json) throws Exception {
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
                        ", 姓名: " + uname +
                        ", 性别: " + gender +
                        ", 年龄组: " + ageGroup +
                        ", 星座: " + constellation +
                        ", 生日: " + birthday);
                return new UserInfoData(userId, uname, gender, ageGroup, constellation, birthday, ts);
            } catch (Exception e) {
                System.err.println("❌ 解析 UserInfo 数据失败: " + json);
                System.err.println("❌ 错误信息: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
    }

    /**
     * Comment-Log CoProcessFunction 连接处理器 - 基于 UserID，设备信息去重，基于变化输出
     */
    public static class CommentLogCoProcessFunction extends CoProcessFunction<CommentData, LogData, CommentLogJoinResult> {

        private transient ValueState<CommentData> commentState;
        private transient ValueState<List<LogData>> logsState;
        private transient ValueState<Set<DeviceInfo>> userDevicesState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<CommentData> commentDescriptor =
                    new ValueStateDescriptor<>("commentState", CommentData.class);
            commentState = getRuntimeContext().getState(commentDescriptor);

            ValueStateDescriptor<List<LogData>> logsDescriptor =
                    new ValueStateDescriptor<>("logsState", TypeInformation.of(new TypeHint<List<LogData>>() {}));
            logsState = getRuntimeContext().getState(logsDescriptor);

            ValueStateDescriptor<Set<DeviceInfo>> devicesDescriptor =
                    new ValueStateDescriptor<>("userDevicesState", TypeInformation.of(new TypeHint<Set<DeviceInfo>>() {}));
            userDevicesState = getRuntimeContext().getState(devicesDescriptor);
        }

        @Override
        public void processElement1(CommentData comment, Context ctx, Collector<CommentLogJoinResult> out) throws Exception {
            System.out.println("🔵 处理 Comment - UserID: " + comment.userId);

            commentState.update(comment);

            List<LogData> logs = logsState.value();
            Set<DeviceInfo> userDevices = userDevicesState.value();
            if (userDevices == null) {
                userDevices = new HashSet<>();
            }

            if (logs != null && !logs.isEmpty()) {
                // 选择最新的Log进行输出
                LogData latestLog = getLatestLog(logs);
                if (comment.userId.equals(latestLog.userId)) {
                    CommentLogJoinResult result = new CommentLogJoinResult(comment, latestLog, userDevices);
                    System.out.println("🎯 Comment-Log JOIN 成功，用户设备数量: " + result.devices.size());
                    out.collect(result);
                }
            }
        }

        @Override
        public void processElement2(LogData log, Context ctx, Collector<CommentLogJoinResult> out) throws Exception {
            System.out.println("🟢 处理 Log - UserID: " + log.userId + ", 类型: " + log.logType);

            // 更新设备信息状态
            Set<DeviceInfo> userDevices = userDevicesState.value();
            if (userDevices == null) {
                userDevices = new HashSet<>();
            }

            boolean deviceAdded = false;
            if (log.deviceInfo != null) {
                // 使用分组逻辑添加设备（brand+plat去重）
                deviceAdded = userDevices.add(log.deviceInfo);
                if (deviceAdded) {
                    System.out.println("📱 用户 " + log.userId + " 新增分组设备 - 品牌: " + log.deviceInfo.brand + ", 平台: " + log.deviceInfo.plat);
                    // 打印分组统计
                    DeviceGroupUtils.printGroupStats(userDevices);
                } else {
                    System.out.println("⏭️ 跳过重复分组设备 - 品牌: " + log.deviceInfo.brand + ", 平台: " + log.deviceInfo.plat);
                }
            }
            userDevicesState.update(userDevices);

            // 更新Log列表
            List<LogData> logs = logsState.value();
            if (logs == null) {
                logs = new ArrayList<>();
            }
            logs.add(log);
            // 只保留最新的20条Log，避免内存溢出
            if (logs.size() > 20) {
                logs = logs.subList(logs.size() - 20, logs.size());
            }
            logsState.update(logs);

            CommentData comment = commentState.value();
            if (comment != null && comment.userId.equals(log.userId)) {
                CommentLogJoinResult result = new CommentLogJoinResult(comment, log, userDevices);
                System.out.println("🎯 Comment-Log JOIN 成功，用户分组设备数量: " + result.devices.size());
                out.collect(result);
            }
        }


        /**
         * 获取最新的Log记录
         */
        private LogData getLatestLog(List<LogData> logs) {
            LogData latest = logs.get(0);
            for (LogData log : logs) {
                if (log.ts > latest.ts) {
                    latest = log;
                }
            }
            return latest;
        }
    }


    /**
     * Comment-Log-UserInfo CoProcessFunction 连接处理器 - 基于 UserID，基于数据变化输出
     */
    public static class CommentLogUserInfoCoProcessFunction extends CoProcessFunction<CommentLogJoinResult, UserInfoData, FinalJoinResult> {

        private transient ValueState<CommentLogJoinResult> commentLogState;
        private transient ValueState<UserInfoData> userInfoState;
        private transient ValueState<Long> lastOutputTimeState;
        private transient ValueState<Set<DeviceInfo>> lastOutputDevicesState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<CommentLogJoinResult> commentLogDescriptor =
                    new ValueStateDescriptor<>("commentLogState", CommentLogJoinResult.class);
            commentLogState = getRuntimeContext().getState(commentLogDescriptor);

            ValueStateDescriptor<UserInfoData> userInfoDescriptor =
                    new ValueStateDescriptor<>("userInfoState", UserInfoData.class);
            userInfoState = getRuntimeContext().getState(userInfoDescriptor);

            ValueStateDescriptor<Long> lastOutputTimeDescriptor =
                    new ValueStateDescriptor<>("lastOutputTimeState", Long.class);
            lastOutputTimeState = getRuntimeContext().getState(lastOutputTimeDescriptor);

            ValueStateDescriptor<Set<DeviceInfo>> lastOutputDevicesDescriptor =
                    new ValueStateDescriptor<>("lastOutputDevicesState", TypeInformation.of(new TypeHint<Set<DeviceInfo>>() {}));
            lastOutputDevicesState = getRuntimeContext().getState(lastOutputDevicesDescriptor);
        }

        @Override
        public void processElement1(CommentLogJoinResult commentLog, Context ctx, Collector<FinalJoinResult> out) throws Exception {
            System.out.println("🔵🟢 处理 Comment-Log 结果 - UserID: " + commentLog.userId + ", 设备数量: " + commentLog.devices.size());

            commentLogState.update(commentLog);

            UserInfoData userInfo = userInfoState.value();
            if (userInfo != null && commentLog.userId.equals(userInfo.userId)) {
                // 基于数据变化判断是否需要输出
                if (shouldOutput(ctx.timerService().currentProcessingTime(), commentLog)) {
                    System.out.println("🎯 日志:三流 JOIN 成功，用户设备数量: " + commentLog.devices.size());
                    out.collect(new FinalJoinResult(commentLog.comment, commentLog.log, userInfo, commentLog.devices));
                    updateOutputState(ctx.timerService().currentProcessingTime(), commentLog.devices);
                } else {
                    System.out.println("⏸️ 数据无显著变化，跳过输出 - UserID: " + commentLog.userId);
                }
            }
        }

        @Override
        public void processElement2(UserInfoData userInfo, Context ctx, Collector<FinalJoinResult> out) throws Exception {
            System.out.println("🟣 处理 UserInfo - UserID: " + userInfo.userId);

            userInfoState.update(userInfo);

            CommentLogJoinResult commentLog = commentLogState.value();
            if (commentLog != null && commentLog.userId.equals(userInfo.userId)) {
                // 基于数据变化判断是否需要输出
                if (shouldOutput(ctx.timerService().currentProcessingTime(), commentLog)) {
                    System.out.println("🎯 最终三流 JOIN 成功，用户设备数量: " + commentLog.devices.size());
                    out.collect(new FinalJoinResult(commentLog.comment, commentLog.log, userInfo, commentLog.devices));
                    updateOutputState(ctx.timerService().currentProcessingTime(), commentLog.devices);
                } else {
                    System.out.println("⏸️ 数据无显著变化，跳过输出 - UserID: " + commentLog.userId);
                }
            }
        }

        /**
         * 基于数据变化判断是否需要输出
         */
        private boolean shouldOutput(long currentTime, CommentLogJoinResult newResult) {
            try {
                Long lastOutputTime = lastOutputTimeState.value();
                Set<DeviceInfo> lastOutputDevices = lastOutputDevicesState.value();

                // 第一次输出
                if (lastOutputTime == null || lastOutputDevices == null) {
                    System.out.println("🆕 首次输出用户数据: " + newResult.userId);
                    return true;
                }

                // 检查设备数量变化
                boolean deviceCountChanged = newResult.devices.size() != lastOutputDevices.size();

                // 检查设备具体变化（新增或减少设备）
                boolean deviceDetailsChanged = !newResult.devices.equals(lastOutputDevices);

                // 检查Log类型是否为重要类型（如支付、登录等）
                boolean importantLogType = isImportantLogType(newResult.log.logType);

                // 检查时间间隔（30秒强制输出一次，确保数据更新）
                boolean timeIntervalExceeded = (currentTime - lastOutputTime) > 60000;

                // 输出条件：设备变化 或 重要日志类型 或 时间间隔超过30秒
                boolean shouldOutput = deviceCountChanged || deviceDetailsChanged || importantLogType || timeIntervalExceeded;

                if (shouldOutput) {
                    System.out.println("📊 输出条件满足 - " +
                            "设备数量变化: " + deviceCountChanged + " (" + lastOutputDevices.size() + " -> " + newResult.devices.size() + "), " +
                            "设备详情变化: " + deviceDetailsChanged + ", " +
                            "重要日志类型: " + importantLogType + " (" + newResult.log.logType + "), " +
                            "时间间隔: " + timeIntervalExceeded);
                }

                return shouldOutput;

            } catch (Exception e) {
                System.err.println("❌ 判断输出条件时出错: " + e.getMessage());
                return true; // 出错时保守输出
            }
        }

        /**
         * 判断是否为重要日志类型
         */
        private boolean isImportantLogType(String logType) {
            if (logType == null) return false;

            // 定义重要日志类型
            Set<String> importantLogTypes = new HashSet<>();
            importantLogTypes.add("payment");     // 支付
            importantLogTypes.add("login");       // 登录
            importantLogTypes.add("purchase");    // 购买
            importantLogTypes.add("checkout");    // 结算
            importantLogTypes.add("order");       // 订单

            return importantLogTypes.contains(logType.toLowerCase());
        }

        /**
         * 更新输出状态
         */
        private void updateOutputState(long currentTime, Set<DeviceInfo> currentDevices) throws IOException {
            lastOutputTimeState.update(currentTime);
            lastOutputDevicesState.update(new HashSet<>(currentDevices)); // 深拷贝
        }
    }

}