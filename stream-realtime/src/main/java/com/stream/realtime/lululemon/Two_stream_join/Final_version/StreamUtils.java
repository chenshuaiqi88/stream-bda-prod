package com.stream.realtime.lululemon.Two_stream_join.Final_version;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.*;

/**
 * 流处理和工具类  有空值
 */
public class StreamUtils {

    // === 流处理方法 ===

    public static SingleOutputStreamOperator<DataModel.CommentLogJoinResult> joinCommentWithLog(
            DataStream<DataModel.CommentData> commentStream,
            DataStream<DataModel.LogData> logStream) {

        return commentStream
                .keyBy(comment -> comment.userId)
                .connect(logStream.keyBy(log -> log.userId))
                .process(new CommentLogCoProcessFunction())
                .name("comment-log-join");
    }

    public static SingleOutputStreamOperator<DataModel.FinalJoinResult> joinWithUserInfo(
            SingleOutputStreamOperator<DataModel.CommentLogJoinResult> commentLogStream,
            DataStream<DataModel.UserInfoData> userInfoStream) {

        return commentLogStream
                .keyBy(result -> result.userId)
                .connect(userInfoStream.keyBy(userInfo -> userInfo.userId))
                .process(new CommentLogUserInfoCoProcessFunction())
                .name("final-join");
    }

    public static void printFinalResults(DataStream<DataModel.FinalJoinResult> finalJoinedStream) {
        finalJoinedStream
                .map(result -> {
                    String json = result.toJson();
                    System.out.println("🎯 ===== 最终 JOIN 结果 =====");
                    System.out.println("🎯 " + json);
                    System.out.println("🎯 =====================");
                    return json;
                })
                .name("final-join-result-printer");
    }

    // === CoProcessFunction 实现 ===

    public static class CommentLogCoProcessFunction extends org.apache.flink.streaming.api.functions.co.CoProcessFunction<DataModel.CommentData, DataModel.LogData, DataModel.CommentLogJoinResult> {

        private transient ValueState<DataModel.CommentData> commentState;
        private transient ValueState<List<DataModel.LogData>> logsState;
        private transient ValueState<Set<DataModel.DeviceInfo>> userDevicesState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<DataModel.CommentData> commentDescriptor =
                    new ValueStateDescriptor<>("commentState", DataModel.CommentData.class);
            commentState = getRuntimeContext().getState(commentDescriptor);

            ValueStateDescriptor<List<DataModel.LogData>> logsDescriptor =
                    new ValueStateDescriptor<>("logsState",
                            TypeInformation.of(new TypeHint<List<DataModel.LogData>>() {}));
            logsState = getRuntimeContext().getState(logsDescriptor);

            ValueStateDescriptor<Set<DataModel.DeviceInfo>> devicesDescriptor =
                    new ValueStateDescriptor<>("userDevicesState",
                            TypeInformation.of(new TypeHint<Set<DataModel.DeviceInfo>>() {}));
            userDevicesState = getRuntimeContext().getState(devicesDescriptor);
        }

        @Override
        public void processElement1(DataModel.CommentData comment, Context ctx, org.apache.flink.util.Collector<DataModel.CommentLogJoinResult> out) throws Exception {
            System.out.println("🔵 处理 Comment - UserID: " + comment.userId);
            commentState.update(comment);

            List<DataModel.LogData> logs = logsState.value();
            Set<DataModel.DeviceInfo> userDevices = userDevicesState.value();
            if (userDevices == null) {
                userDevices = new HashSet<>();
            }

            if (logs != null && !logs.isEmpty()) {
                for (DataModel.LogData log : logs) {
                    if (comment.userId.equals(log.userId)) {
                        DataModel.CommentLogJoinResult result = new DataModel.CommentLogJoinResult(comment, log, userDevices);
                        System.out.println("🎯 Comment-Log JOIN 成功，日志类型: " + log.logType + ", 用户设备数量: " + result.devices.size());

                        if ("search".equals(log.logType)) {
                            System.out.println("🔍 SEARCH 日志 JOIN 完成，Keywords: " + log.keywords);
                        }

                        out.collect(result);
                    }
                }
            }
        }

        @Override
        public void processElement2(DataModel.LogData log, Context ctx, org.apache.flink.util.Collector<DataModel.CommentLogJoinResult> out) throws Exception {
            System.out.println("🟢 处理 Log - UserID: " + log.userId + ", 类型: " + log.logType);

            // 更新设备信息状态
            Set<DataModel.DeviceInfo> userDevices = userDevicesState.value();
            if (userDevices == null) {
                userDevices = new HashSet<>();
            }

            if (log.deviceInfo != null) {
                boolean deviceAdded = userDevices.add(log.deviceInfo);
                if (deviceAdded) {
                    System.out.println("📱 用户 " + log.userId + " 新增分组设备 - 品牌: " + log.deviceInfo.brand + ", 平台: " + log.deviceInfo.plat);
                    DeviceUtils.printGroupStats(userDevices);
                } else {
                    System.out.println("⏭️ 跳过重复分组设备 - 品牌: " + log.deviceInfo.brand + ", 平台: " + log.deviceInfo.plat);
                }
            }
            userDevicesState.update(userDevices);

            // 更新Log列表
            List<DataModel.LogData> logs = logsState.value();
            if (logs == null) {
                logs = new ArrayList<>();
            }
            logs.add(log);

            if (logs.size() > 50) {
                logs = logs.subList(logs.size() - 50, logs.size());
            }
            logsState.update(logs);

            DataModel.CommentData comment = commentState.value();
            if (comment != null && comment.userId.equals(log.userId)) {
                DataModel.CommentLogJoinResult result = new DataModel.CommentLogJoinResult(comment, log, userDevices);
                System.out.println("🎯 Comment-Log JOIN 成功，日志类型: " + log.logType + ", 用户分组设备数量: " + result.devices.size());

                if ("search".equals(log.logType)) {
                    System.out.println("🔍 SEARCH 日志 JOIN 完成，Keywords: " + log.keywords);
                }

                out.collect(result);
            }
        }

        private DataModel.LogData getLatestLog(List<DataModel.LogData> logs) {
            if (logs == null || logs.isEmpty()) {
                return null;
            }

            DataModel.LogData latest = logs.get(0);
            for (DataModel.LogData log : logs) {
                if (log.ts > latest.ts) {
                    latest = log;
                }
            }
            return latest;
        }
    }

    public static class CommentLogUserInfoCoProcessFunction extends org.apache.flink.streaming.api.functions.co.CoProcessFunction<DataModel.CommentLogJoinResult, DataModel.UserInfoData, DataModel.FinalJoinResult> {

        private transient ValueState<DataModel.CommentLogJoinResult> commentLogState;
        private transient ValueState<DataModel.UserInfoData> userInfoState;
        private transient ValueState<Long> lastOutputTimeState;
        private transient ValueState<Set<DataModel.DeviceInfo>> lastOutputDevicesState;
        private transient ValueState<String> lastOutputLogTypeState;
        private transient ValueState<DataModel.UserBehaviorHistory> behaviorHistoryState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);

            commentLogState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("commentLogState", DataModel.CommentLogJoinResult.class));
            userInfoState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("userInfoState", DataModel.UserInfoData.class));
            lastOutputTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastOutputTimeState", Long.class));
            lastOutputDevicesState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastOutputDevicesState",
                            TypeInformation.of(new TypeHint<Set<DataModel.DeviceInfo>>() {})));
            lastOutputLogTypeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastOutputLogTypeState", String.class));
            
            ValueStateDescriptor<DataModel.UserBehaviorHistory> behaviorHistoryDescriptor = 
                    new ValueStateDescriptor<>(
                        "behaviorHistoryState", 
                        TypeInformation.of(new TypeHint<DataModel.UserBehaviorHistory>() {})
                    );
            behaviorHistoryState = getRuntimeContext().getState(behaviorHistoryDescriptor);
        }

        @Override
        public void processElement1(DataModel.CommentLogJoinResult commentLog, Context ctx, org.apache.flink.util.Collector<DataModel.FinalJoinResult> out) throws Exception {
            System.out.println("🔵🟢 处理 Comment-Log 结果 - UserID: " + commentLog.userId +
                    ", 日志类型: " + commentLog.log.logType +
                    ", Keywords: " + commentLog.log.keywords);

            // 更新行为历史
            updateBehaviorHistory(commentLog);
            DataModel.UserBehaviorHistory behaviorHistory = behaviorHistoryState.value();

            // 注册每日定时器
            registerDailyTimer(ctx);

            // 特别处理 search 日志
            if ("search".equals(commentLog.log.logType)) {
                System.out.println("🔍 处理 SEARCH 日志结果 - 立即输出");
                DataModel.UserInfoData userInfo = userInfoState.value();
                if (userInfo != null && commentLog.userId.equals(userInfo.userId)) {
                    System.out.println("🎯🔍 SEARCH 日志三流 JOIN 成功，Keywords: " + commentLog.log.keywords);
                    out.collect(new DataModel.FinalJoinResult(commentLog.comment, commentLog.log, userInfo, commentLog.devices, behaviorHistory));
                    updateOutputState(ctx.timerService().currentProcessingTime(), commentLog.devices, commentLog.log.logType);
                }
            } else {
                commentLogState.update(commentLog);
                DataModel.UserInfoData userInfo = userInfoState.value();
                if (userInfo != null && commentLog.userId.equals(userInfo.userId)) {
                    if (shouldOutput(ctx.timerService().currentProcessingTime(), commentLog)) {
                        System.out.println("🎯 三流 JOIN 成功，日志类型: " + commentLog.log.logType);
                        out.collect(new DataModel.FinalJoinResult(commentLog.comment, commentLog.log, userInfo, commentLog.devices, behaviorHistory));
                        updateOutputState(ctx.timerService().currentProcessingTime(), commentLog.devices, commentLog.log.logType);
                    } else {
                        System.out.println("⏸️ 数据无显著变化，跳过输出 - UserID: " + commentLog.userId + ", 类型: " + commentLog.log.logType);
                    }
                }
            }
        }

        @Override
        public void processElement2(DataModel.UserInfoData userInfo, Context ctx, org.apache.flink.util.Collector<DataModel.FinalJoinResult> out) throws Exception {
            System.out.println("🟣 处理 UserInfo - UserID: " + userInfo.userId);
            userInfoState.update(userInfo);

            // 注册每日定时器
            registerDailyTimer(ctx);

            DataModel.CommentLogJoinResult commentLog = commentLogState.value();
            DataModel.UserBehaviorHistory behaviorHistory = behaviorHistoryState.value();

            if (commentLog != null && commentLog.userId.equals(userInfo.userId)) {
                if ("search".equals(commentLog.log.logType)) {
                    System.out.println("🎯🔍 UserInfo 触发 SEARCH 日志输出，Keywords: " + commentLog.log.keywords);
                    out.collect(new DataModel.FinalJoinResult(commentLog.comment, commentLog.log, userInfo, commentLog.devices, behaviorHistory));
                    updateOutputState(ctx.timerService().currentProcessingTime(), commentLog.devices, commentLog.log.logType);
                } else if (shouldOutput(ctx.timerService().currentProcessingTime(), commentLog)) {
                    System.out.println("🎯 最终三流 JOIN 成功，日志类型: " + commentLog.log.logType);
                    out.collect(new DataModel.FinalJoinResult(commentLog.comment, commentLog.log, userInfo, commentLog.devices, behaviorHistory));
                    updateOutputState(ctx.timerService().currentProcessingTime(), commentLog.devices, commentLog.log.logType);
                } else {
                    System.out.println("⏸️ 数据无显著变化，跳过输出 - UserID: " + commentLog.userId + ", 类型: " + commentLog.log.logType);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, org.apache.flink.util.Collector<DataModel.FinalJoinResult> out) throws Exception {
            DataModel.CommentLogJoinResult commentLog = commentLogState.value();
            DataModel.UserInfoData userInfo = userInfoState.value();
            DataModel.UserBehaviorHistory behaviorHistory = behaviorHistoryState.value();
            
            if (commentLog != null && userInfo != null && behaviorHistory != null && 
                (!behaviorHistory.getLoginTimes().isEmpty() || !behaviorHistory.getSearchRecords().isEmpty())) {
                
                System.out.println("⏰ 定时批量输出用户行为统计 - UserID: " + userInfo.userId + 
                    ", 登录次数: " + behaviorHistory.getLoginTimes().size() + 
                    ", 搜索记录: " + behaviorHistory.getSearchRecords().size());
                
                out.collect(new DataModel.FinalJoinResult(commentLog.comment, commentLog.log, userInfo, commentLog.devices, behaviorHistory));
                updateOutputState(ctx.timerService().currentProcessingTime(), commentLog.devices, "daily_batch");
                
                registerDailyTimer(ctx);
            }
        }

        private void updateBehaviorHistory(DataModel.CommentLogJoinResult commentLog) throws Exception {
            DataModel.UserBehaviorHistory history = behaviorHistoryState.value();
            if (history == null) {
                history = new DataModel.UserBehaviorHistory();
                System.out.println("📊 初始化用户行为历史 - UserID: " + commentLog.userId);
            }

            // 记录登录行为
            if ("login".equals(commentLog.log.logType)) {
                String loginTime = formatTimestamp(commentLog.log.ts);
                history.addLoginTime(loginTime);
                System.out.println("📝 记录登录行为 - UserID: " + commentLog.userId + ", 时间: " + loginTime +
                    ", 总登录次数: " + history.getLoginTimes().size());
            }

            // 记录搜索行为
            if ("search".equals(commentLog.log.logType) && commentLog.log.keywords != null && !commentLog.log.keywords.isEmpty()) {
                DataModel.SearchRecord searchRecord = new DataModel.SearchRecord(
                    commentLog.log.keywords, commentLog.log.ts, commentLog.log.logType
                );
                history.addSearchRecord(searchRecord);
                System.out.println("🔍 记录搜索行为 - UserID: " + commentLog.userId +
                    ", 关键词: " + commentLog.log.keywords +
                    ", 时间: " + formatTimestamp(commentLog.log.ts) +
                    ", 总搜索记录: " + history.getSearchRecords().size());
            }

            behaviorHistoryState.update(history);
        }

        private void registerDailyTimer(Context ctx)  {
            long now = ctx.timerService().currentProcessingTime();
            long oneDay = 24 * 60 * 60 * 1000L;
            long nextTrigger = ((now / oneDay) + 1) * oneDay;
            ctx.timerService().registerProcessingTimeTimer(nextTrigger);
            System.out.println("⏰ 注册每日定时器 - 下次触发: " + formatTimestamp(nextTrigger));
        }

        private String formatTimestamp(Long timestamp) {
            if (timestamp == null) return "";
            try {
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return sdf.format(new java.util.Date(timestamp));
            } catch (Exception e) {
                return String.valueOf(timestamp);
            }
        }

        private boolean shouldOutput(long currentTime, DataModel.CommentLogJoinResult newResult) {
            try {
                Long lastOutputTime = lastOutputTimeState.value();
                Set<DataModel.DeviceInfo> lastOutputDevices = lastOutputDevicesState.value();
                String lastOutputLogType = lastOutputLogTypeState.value();

                if (lastOutputTime == null || lastOutputDevices == null || lastOutputLogType == null) {
                    System.out.println("🆕 首次输出用户数据: " + newResult.userId + ", 类型: " + newResult.log.logType);
                    return true;
                }

                boolean deviceChanged = DeviceUtils.hasDeviceChanged(newResult.devices, lastOutputDevices);
                boolean importantLogType = LogTypeUtils.isImportantLogType(newResult.log.logType);
                boolean timeIntervalExceeded = (currentTime - lastOutputTime) > 60000;
                boolean logTypeChanged = !newResult.log.logType.equals(lastOutputLogType);
                boolean isSearch = "search".equals(newResult.log.logType);

                boolean shouldOutput = deviceChanged || importantLogType || timeIntervalExceeded || logTypeChanged || isSearch;

                if (shouldOutput) {
                    System.out.println("📊 输出条件满足 - 设备变化: " + deviceChanged +
                            ", 重要日志: " + importantLogType + " (" + newResult.log.logType + ")" +
                            ", 时间间隔: " + timeIntervalExceeded +
                            ", 日志类型变化: " + logTypeChanged +
                            ", 搜索日志: " + isSearch);
                }
                return shouldOutput;

            } catch (Exception e) {
                System.err.println("❌ 判断输出条件时出错: " + e.getMessage());
                return true;
            }
        }

        private void updateOutputState(long currentTime, Set<DataModel.DeviceInfo> currentDevices, String logType) throws java.io.IOException {
            lastOutputTimeState.update(currentTime);
            lastOutputDevicesState.update(new HashSet<>(currentDevices));
            lastOutputLogTypeState.update(logType);
        }
    }

    // === 工具方法 ===

    public static class DeviceUtils {
        public static void printGroupStats(Set<DataModel.DeviceInfo> devices) {
            if (devices == null || devices.isEmpty()) {
                System.out.println("📊 无设备信息");
                return;
            }

            Map<String, List<DataModel.DeviceInfo>> groupMap = new HashMap<>();
            for (DataModel.DeviceInfo device : devices) {
                String groupKey = device.getGroupKey();
                groupMap.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(device);
            }

            System.out.println("📊 设备分组统计 - 总数: " + devices.size() + ", 分组数: " + groupMap.size());
            for (Map.Entry<String, List<DataModel.DeviceInfo>> entry : groupMap.entrySet()) {
                System.out.println("   📍 " + entry.getKey() + ": " + entry.getValue().size() + " 个设备");
            }
        }

        public static boolean hasDeviceChanged(Set<DataModel.DeviceInfo> currentDevices, Set<DataModel.DeviceInfo> lastDevices) {
            if (lastDevices == null) return true;
            return !currentDevices.equals(lastDevices);
        }
    }

    public static class LogTypeUtils {
        private static final Set<String> IMPORTANT_LOG_TYPES;

        static {
            IMPORTANT_LOG_TYPES = new HashSet<>();
            IMPORTANT_LOG_TYPES.add("payment");
            IMPORTANT_LOG_TYPES.add("login");
            IMPORTANT_LOG_TYPES.add("purchase");
            IMPORTANT_LOG_TYPES.add("checkout");
            IMPORTANT_LOG_TYPES.add("order");
            IMPORTANT_LOG_TYPES.add("search");
        }

        public static boolean isImportantLogType(String logType) {
            if (logType == null) return false;
            return IMPORTANT_LOG_TYPES.contains(logType.toLowerCase());
        }
    }

    public static class JsonUtils {
        public static String generateFinalResultJson(DataModel.FinalJoinResult result) {
            JSONObject json = new JSONObject();

            // 基础用户信息
            String userId = result.userInfo != null ? result.userInfo.userId :
                    result.comment != null ? result.comment.userId :
                            result.log != null ? result.log.userId : "unknown";

            // 元数据信息
            json.put("userid", userId);
            json.put("date", getCurrentDate());
            json.put("aggregation_type", "daily");
            json.put("ds", extractDs(result));
            json.put("ts", extractTs(result));
            json.put("process_time", getCurrentDateTime());
            json.put("data_type", "historical");
            json.put("is_latest", true);

            // user_info 部分
            json.put("user_info", generateUserInfo(result));

            // statistics 部分
            json.put("statistics", generateStatistics(result));

            // 其他字段
            json.put("consumption_level", extractConsumptionLevel(result));
            json.put("search_info", generateSearchInfo(result));
            json.put("shopping_gender", generateShoppingGender(result));
            json.put("last_update_time", System.currentTimeMillis());
            json.put("total_records", calculateTotalRecords(result));
            json.put("unique_comments", calculateUniqueComments(result));
            json.put("unique_behaviors", calculateUniqueBehaviors(result));
            json.put("data_timestamp", extractDataTimestamp(result));
            json.put("data_datetime", formatTimestamp(extractDataTimestamp(result)));

            return json.toJSONString();
        }

        private static JSONObject generateUserInfo(DataModel.FinalJoinResult result) {
            JSONObject userInfo = new JSONObject();
            
            String userId = result.userInfo != null ? result.userInfo.userId :
                    result.comment != null ? result.comment.userId :
                            result.log != null ? result.log.userId : "unknown";

            userInfo.put("userid", userId);
            userInfo.put("username", result.userInfo != null && result.userInfo.uname != null ? 
                    result.userInfo.uname : "");
            userInfo.put("date", getCurrentDate());
            userInfo.put("user_base_info", generateUserBaseInfo(result));
            userInfo.put("login_time", extractLoginTimes(result));

            return userInfo;
        }

        private static JSONObject generateUserBaseInfo(DataModel.FinalJoinResult result) {
            JSONObject baseInfo = new JSONObject();

            if (result.userInfo != null) {
                baseInfo.put("birthday", result.userInfo.birthday != null ? result.userInfo.birthday : "");
                baseInfo.put("decade", result.userInfo.ageGroup != null ? result.userInfo.ageGroup : "");

                // 星座处理
                String constellation = result.userInfo.constellation != null ? result.userInfo.constellation : "";
                if (!constellation.isEmpty() && !constellation.endsWith("座")) {
                    constellation = constellation + "座";
                }
                baseInfo.put("zodiac_sign", constellation);
                
                // 计算年龄
                int age = calculateAgeFromBirthday(result.userInfo.birthday);
                baseInfo.put("age", String.valueOf(age));
                baseInfo.put("year_best", String.valueOf(age));

                // 性别编码转换
                String genderText = "未知";
                if (result.userInfo.gender != null) {
                    if (result.userInfo.gender == 1) {
                        genderText = "男";
                    } else if (result.userInfo.gender == 0) {
                        genderText = "女";
                    }
                }
                baseInfo.put("gender", genderText);
                baseInfo.put("weight", "");

            } else {
                baseInfo.put("birthday", "");
                baseInfo.put("decade", "");
                baseInfo.put("zodiac_sign", "");
                baseInfo.put("age", "0");
                baseInfo.put("year_best", "0");
                baseInfo.put("gender", "");
                baseInfo.put("weight", "");
            }

            return baseInfo;
        }

        private static JSONObject generateStatistics(DataModel.FinalJoinResult result) {
            JSONObject statistics = new JSONObject();
            
            statistics.put("behavior_analysis", generateBehaviorAnalysis(result));
            statistics.put("search_analysis", generateSearchAnalysis(result));
            statistics.put("comment_analysis", generateCommentAnalysis(result));
            statistics.put("order_analysis", generateOrderAnalysis(result));
            statistics.put("product_analysis", generateProductAnalysis(result));
            statistics.put("environment_analysis", generateEnvironmentAnalysis(result));
            statistics.put("time_analysis", generateTimeAnalysis(result));

            return statistics;
        }

        private static JSONObject generateBehaviorAnalysis(DataModel.FinalJoinResult result) {
            JSONObject behaviorAnalysis = new JSONObject();
            
            Set<String> behaviorTypes = new HashSet<>();
            Map<String, Integer> behaviorCounts = new HashMap<>();
            int totalBehaviors = 0;

            if (result.log != null) {
                behaviorTypes.add(result.log.logType);
                behaviorCounts.put(result.log.logType, 1);
                totalBehaviors = 1;
            }

            behaviorAnalysis.put("all_behavior_types", new ArrayList<>(behaviorTypes));
            behaviorAnalysis.put("behavior_counts", behaviorCounts);
            behaviorAnalysis.put("total_behaviors", totalBehaviors);

            return behaviorAnalysis;
        }

        private static JSONObject generateSearchAnalysis(DataModel.FinalJoinResult result) {
            JSONObject searchAnalysis = new JSONObject();
            
            List<String> allKeywords = new ArrayList<>();
            if (result.log != null && "search".equals(result.log.logType) && result.log.keywords != null) {
                allKeywords.addAll(result.log.keywords);
            }

            Map<String, Integer> searchCounts = new HashMap<>();
            for (String keyword : allKeywords) {
                searchCounts.put(keyword, searchCounts.getOrDefault(keyword, 0) + 1);
            }

            searchAnalysis.put("all_searched_keywords", allKeywords);
            searchAnalysis.put("total_searched_keywords", allKeywords.size());
            searchAnalysis.put("search_counts", searchCounts);

            return searchAnalysis;
        }

        private static JSONObject generateCommentAnalysis(DataModel.FinalJoinResult result) {
            JSONObject commentAnalysis = new JSONObject();
            
            JSONArray allComments = new JSONArray();
            Set<String> commentedProducts = new HashSet<>();
            int totalComments = 0;
            int insultingComments = 0;
            int positiveComments = 0;

            if (result.comment != null) {
                JSONObject commentJson = new JSONObject();
                commentJson.put("sensitive_level", result.comment.sensitiveLevel != null ? result.comment.sensitiveLevel : "");
                commentJson.put("is_insulting", result.comment.isBlocked != null ? result.comment.isBlocked : false);
                commentJson.put("comment", result.comment.userComment != null ? result.comment.userComment : "");
                commentJson.put("product_name", result.comment.productId != null ? result.comment.productId : "");
                commentJson.put("timestamp", formatTimestamp(result.comment.ts));
                
                allComments.add(commentJson);
                totalComments = 1;
                
                if (result.comment.productId != null) {
                    commentedProducts.add(result.comment.productId);
                }
                
                if (Boolean.TRUE.equals(result.comment.isBlocked)) {
                    insultingComments++;
                } else {
                    positiveComments++;
                }
            }

            commentAnalysis.put("all_comments", allComments);
            commentAnalysis.put("total_comments", totalComments);
            commentAnalysis.put("insulting_comments", insultingComments);
            commentAnalysis.put("positive_comments", positiveComments);
            commentAnalysis.put("commented_products", new ArrayList<>(commentedProducts));
            commentAnalysis.put("is_check_sensitive_comment", 
                result.comment != null && result.comment.triggeredKeyword != null && 
                !result.comment.triggeredKeyword.isEmpty() ? "1" : "0");
            
            JSONArray sensitiveWords = new JSONArray();
            if (result.comment != null && result.comment.triggeredKeyword != null && 
                !result.comment.triggeredKeyword.isEmpty()) {
                sensitiveWords.add(result.comment.triggeredKeyword);
            }
            commentAnalysis.put("sensitive_word", sensitiveWords);

            return commentAnalysis;
        }

        private static JSONObject generateOrderAnalysis(DataModel.FinalJoinResult result) {
            JSONObject orderAnalysis = new JSONObject();
            
            double totalOrderAmount = 0.0;
            double maxOrderAmount = 0.0;
            double minOrderAmount = 0.0;
            int orderCount = 0;
            JSONArray orderAmounts = new JSONArray();

            if (result.comment != null && result.comment.totalAmount != null) {
                totalOrderAmount = result.comment.totalAmount;
                maxOrderAmount = result.comment.totalAmount;
                minOrderAmount = result.comment.totalAmount;
                orderCount = 1;
                orderAmounts.add(result.comment.totalAmount);
            }

            orderAnalysis.put("total_order_amount", totalOrderAmount);
            orderAnalysis.put("average_order_amount", orderCount > 0 ? totalOrderAmount / orderCount : 0);
            orderAnalysis.put("max_order_amount", maxOrderAmount);
            orderAnalysis.put("min_order_amount", minOrderAmount);
            orderAnalysis.put("order_count", orderCount);
            orderAnalysis.put("order_amounts", orderAmounts);

            return orderAnalysis;
        }

        private static JSONObject generateProductAnalysis(DataModel.FinalJoinResult result) {
            JSONObject productAnalysis = new JSONObject();
            
            Set<String> viewedProducts = new HashSet<>();
            Set<String> commentedProducts = new HashSet<>();
            Set<String> productCategories = new HashSet<>();
            Map<String, Integer> productViewCounts = new HashMap<>();
            Map<String, Double> categorySpend = new HashMap<>();

            if (result.log != null && result.log.productId != null) {
                viewedProducts.add(result.log.productId);
                productViewCounts.put(result.log.productId, 1);
                
                // 简单的品类提取
                if (result.log.productId.contains("AIM")) {
                    productCategories.add("AIM");
                    categorySpend.put("AIM", 0.0);
                } else if (result.log.productId.contains("Align")) {
                    productCategories.add("Align™");
                    categorySpend.put("Align™", 0.0);
                } else if (result.log.productId.contains("Fast")) {
                    productCategories.add("Fast and");
                    categorySpend.put("Fast and", 0.0);
                }
            }

            if (result.comment != null && result.comment.productId != null) {
                commentedProducts.add(result.comment.productId);
            }

            // 如果有消费金额，更新品类消费
            if (result.comment != null && result.comment.totalAmount != null) {
                for (String category : productCategories) {
                    categorySpend.put(category, result.comment.totalAmount);
                }
            }

            productAnalysis.put("viewed_products", new ArrayList<>(viewedProducts));
            productAnalysis.put("commented_products", new ArrayList<>(commentedProducts));
            productAnalysis.put("product_categories", new ArrayList<>(productCategories));
            productAnalysis.put("product_view_counts", productViewCounts);
            productAnalysis.put("category_spend", categorySpend);

            return productAnalysis;
        }

        private static JSONObject generateEnvironmentAnalysis(DataModel.FinalJoinResult result) {
            JSONObject environmentAnalysis = new JSONObject();
            
            JSONArray devices = new JSONArray();
            Set<String> networks = new HashSet<>();
            Set<String> ips = new HashSet<>();

            // 设备信息
            if (result.devices != null && !result.devices.isEmpty()) {
                for (DataModel.DeviceInfo device : result.devices) {
                    JSONObject deviceJson = new JSONObject();
                    deviceJson.put("brand", device.brand);
                    deviceJson.put("plat", device.plat);
                    deviceJson.put("plat-v", device.platv);
                    deviceJson.put("soft-v", device.softv);
                    deviceJson.put("device", device.device);
                    devices.add(deviceJson);
                }
            }

            // 网络和IP信息
            if (result.log != null && result.log.ipAddress != null) {
                ips.add(result.log.ipAddress);
                networks.add("unknown");
            }

            environmentAnalysis.put("devices", devices);
            environmentAnalysis.put("networks", new ArrayList<>(networks));
            environmentAnalysis.put("ips", new ArrayList<>(ips));

            return environmentAnalysis;
        }

        private static JSONObject generateTimeAnalysis(DataModel.FinalJoinResult result) {
            JSONObject timeAnalysis = new JSONObject();
            
            String firstActivity = "";
            String lastActivity = "";
            String firstComment = "";
            String lastComment = "";
            JSONArray loginTimes = extractLoginTimes(result);

            if (result.log != null && result.log.ts != null) {
                String activityTime = formatTimestamp(result.log.ts);
                firstActivity = activityTime;
                lastActivity = activityTime;
            }

            if (result.comment != null && result.comment.ts != null) {
                String commentTime = formatTimestamp(result.comment.ts);
                firstComment = commentTime;
                lastComment = commentTime;
            }

            timeAnalysis.put("first_activity", firstActivity);
            timeAnalysis.put("last_activity", lastActivity);
            timeAnalysis.put("first_comment", firstComment);
            timeAnalysis.put("last_comment", lastComment);
            timeAnalysis.put("login_time", loginTimes);

            return timeAnalysis;
        }

        private static JSONArray extractLoginTimes(DataModel.FinalJoinResult result) {
            JSONArray loginTimes = new JSONArray();

            if (result.getBehaviorHistory() != null && !result.getBehaviorHistory().getLoginTimes().isEmpty()) {
                List<String> sortedLogins = new ArrayList<>(result.getBehaviorHistory().getLoginTimes());
                sortedLogins.sort(Collections.reverseOrder());
                int maxLogins = Math.min(sortedLogins.size(), 20);
                for (int i = 0; i < maxLogins; i++) {
                    loginTimes.add(sortedLogins.get(i));
                }
            } else if (result.log != null && "login".equals(result.log.logType)) {
                String loginTime = formatTimestamp(result.log.ts);
                loginTimes.add(loginTime);
            }

            return loginTimes;
        }

        private static JSONObject generateSearchInfo(DataModel.FinalJoinResult result) {
            JSONObject searchInfo = new JSONObject();

            List<String> keywords = new ArrayList<>();
            if (result.log != null && "search".equals(result.log.logType) && result.log.keywords != null) {
                keywords.addAll(result.log.keywords);
            }

            searchInfo.put("keywords", keywords);
            searchInfo.put("search_count", keywords.size());

            return searchInfo;
        }

        private static JSONObject generateShoppingGender(DataModel.FinalJoinResult result) {
            JSONObject shoppingGender = new JSONObject();

            String shoppingGenderPref = "unknown";
            List<String> shoppingIds = new ArrayList<>();

            if (result.comment != null) {
                String comment = result.comment.userComment != null ? result.comment.userComment.toLowerCase() : "";
                if (comment.contains("女生") || comment.contains("女士") || comment.contains("女款") || comment.contains("女性")) {
                    shoppingGenderPref = "女";
                } else if (comment.contains("男生") || comment.contains("男士") || comment.contains("男款") || comment.contains("男性")) {
                    shoppingGenderPref = "男";
                }

                if (result.comment.orderId != null) {
                    shoppingIds.add(result.comment.orderId);
                }
            }

            if (result.log != null && result.log.orderId != null) {
                if (!shoppingIds.contains(result.log.orderId)) {
                    shoppingIds.add(result.log.orderId);
                }
            }

            shoppingGender.put("gender", shoppingGenderPref);
            shoppingGender.put("shopping_id", shoppingIds);

            return shoppingGender;
        }

        // 辅助方法
        private static String getCurrentDate() {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            return sdf.format(new java.util.Date());
        }

        private static String getCurrentDateTime() {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return sdf.format(new java.util.Date());
        }

        private static int calculateTotalRecords(DataModel.FinalJoinResult result) {
            int count = 0;
            if (result.comment != null) count++;
            if (result.log != null) count++;
            if (result.userInfo != null) count++;
            return count;
        }

        private static int calculateUniqueComments(DataModel.FinalJoinResult result) {
            return result.comment != null ? 1 : 0;
        }

        private static int calculateUniqueBehaviors(DataModel.FinalJoinResult result) {
            return result.log != null ? 1 : 0;
        }

        private static Long extractDataTimestamp(DataModel.FinalJoinResult result) {
            if (result.comment != null && result.comment.ts != null) {
                return result.comment.ts;
            } else if (result.log != null && result.log.ts != null) {
                return result.log.ts;
            }
            return System.currentTimeMillis();
        }

        private static String extractConsumptionLevel(DataModel.FinalJoinResult result) {
            if (result.comment != null && result.comment.consumptionLevel != null) {
                return result.comment.consumptionLevel.toLowerCase();
            }
            return "unknown";
        }

        private static String extractDs(DataModel.FinalJoinResult result) {
            if (result.comment != null && result.comment.ds != null) {
                return result.comment.ds;
            }
            return "";
        }

        private static String extractTs(DataModel.FinalJoinResult result) {
            if (result.comment != null && result.comment.ts != null) {
                return String.valueOf(result.comment.ts);
            } else if (result.log != null && result.log.ts != null) {
                return String.valueOf(result.log.ts);
            }
            return String.valueOf(System.currentTimeMillis());
        }

        /**
         * 根据生日计算年龄
         */
        private static int calculateAgeFromBirthday(String birthday) {
            if (birthday == null || birthday.isEmpty()) {
                return 0;
            }

            try {
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
                java.util.Date birthDate = sdf.parse(birthday);
                java.util.Date currentDate = new java.util.Date();

                java.util.Calendar birthCal = java.util.Calendar.getInstance();
                birthCal.setTime(birthDate);
                java.util.Calendar currentCal = java.util.Calendar.getInstance();
                currentCal.setTime(currentDate);

                int age = currentCal.get(java.util.Calendar.YEAR) - birthCal.get(java.util.Calendar.YEAR);

                if (currentCal.get(java.util.Calendar.MONTH) < birthCal.get(java.util.Calendar.MONTH) ||
                        (currentCal.get(java.util.Calendar.MONTH) == birthCal.get(java.util.Calendar.MONTH) &&
                                currentCal.get(java.util.Calendar.DAY_OF_MONTH) < birthCal.get(java.util.Calendar.DAY_OF_MONTH))) {
                    age--;
                }

                return age;

            } catch (Exception e) {
                System.err.println("❌ 计算年龄失败，生日格式: " + birthday);
                return 0;
            }
        }

        /**
         * 简化版用户等级计算（仅基于消费金额）
         */
        private static String calculateMembershipLevel(DataModel.FinalJoinResult result) {
            if (result.comment != null && result.comment.totalAmount != null) {
                double amount = result.comment.totalAmount;
                if (amount > 1500) return "premium";
                else if (amount > 500) return "regular";
            }
            return "new";
        }

        private static String formatTimestamp(Long timestamp) {
            if (timestamp == null) return "";
            try {
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return sdf.format(new java.util.Date(timestamp));
            } catch (Exception e) {
                return String.valueOf(timestamp);
            }
        }
    }
}