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
 * 流处理和工具类
 */
public class StreamUtils2 {

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
                // 修改：为所有匹配的日志生成JOIN结果，而不仅仅是最新的
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

            // 更新Log列表 - 修改为保留所有类型的日志
            List<DataModel.LogData> logs = logsState.value();
            if (logs == null) {
                logs = new ArrayList<>();
            }
            logs.add(log);

            // 增加缓冲区大小，确保search日志不被过早清除
            if (logs.size() > 50) {  // 从20增加到50
                logs = logs.subList(logs.size() - 50, logs.size());
            }
            logsState.update(logs);

            DataModel.CommentData comment = commentState.value();
            if (comment != null && comment.userId.equals(log.userId)) {
                // 为当前日志立即生成JOIN结果，而不是使用最新日志
                DataModel.CommentLogJoinResult result = new DataModel.CommentLogJoinResult(comment, log, userDevices);
                System.out.println("🎯 Comment-Log JOIN 成功，日志类型: " + log.logType + ", 用户分组设备数量: " + result.devices.size());

                // 特别标记search日志
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

// 在 StreamUtils.java 中添加行为统计相关的类和修改

    public static class CommentLogUserInfoCoProcessFunction extends org.apache.flink.streaming.api.functions.co.CoProcessFunction<DataModel.CommentLogJoinResult, DataModel.UserInfoData, DataModel.FinalJoinResult> {

        private transient ValueState<DataModel.CommentLogJoinResult> commentLogState;
        private transient ValueState<DataModel.UserInfoData> userInfoState;
        private transient ValueState<Long> lastOutputTimeState;
        private transient ValueState<Set<DataModel.DeviceInfo>> lastOutputDevicesState;
        private transient ValueState<String> lastOutputLogTypeState;
        private transient ValueState<DataModel.UserBehaviorHistory> behaviorHistoryState; // 新增：行为历史状态

        // 修改行为历史状态的类型信息
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

            // 使用明确的类型信息
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
                // 其他日志类型按原有逻辑处理
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

            DataModel.CommentLogJoinResult commentLog = commentLogState.value();
            DataModel.UserBehaviorHistory behaviorHistory = behaviorHistoryState.value();

            if (commentLog != null && commentLog.userId.equals(userInfo.userId)) {
                // 特别处理：如果有 search 日志在状态中，优先输出
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
            // 每天触发一次批量输出
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

                // 注册下一个定时器
                registerDailyTimer(ctx);
            }
        }

        // 修改 updateBehaviorHistory 方法，确保完全安全
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
            // 计算下一个整点时间（每天触发一次）
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

        // 原有的 shouldOutput 和 updateOutputState 方法保持不变
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

                // 如果是 search 日志，总是输出（确保 keywords 不被遗漏）
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
            IMPORTANT_LOG_TYPES.add("search");  // 新增这一行
        }

        public static boolean isImportantLogType(String logType) {
            if (logType == null) return false;
            return IMPORTANT_LOG_TYPES.contains(logType.toLowerCase());
        }
    }



    // === 工具方法 ===

    public static class JsonUtils {
        public static String generateFinalResultJson(DataModel.FinalJoinResult result) {
            JSONObject json = new JSONObject();

            // 基础用户信息
            String userId = result.userInfo != null ? result.userInfo.userId :
                    result.comment != null ? result.comment.userId :
                            result.log != null ? result.log.userId : "unknown";

            json.put("userid", userId);

            // username - 统一空值处理
            json.put("username", result.userInfo != null && result.userInfo.uname != null ?
                    result.userInfo.uname : "");

            // user_base_info
            json.put("user_base_info", generateUserBaseInfo(result));

            // login_activity - 使用行为历史数据
            json.put("login_activity", generateLoginActivity(result));

            // financial_profile
            json.put("financial_profile", generateFinancialProfile(result));

            // device_analysis
            json.put("device_analysis", generateDeviceAnalysis(result.devices));

            // search_behavior - 使用行为历史数据
            json.put("search_behavior", generateSearchBehavior(result));

            // category_preferences - 使用行为历史数据
            json.put("category_preferences", generateCategoryPreferences(result));

            // shopping_profile
            json.put("shopping_profile", generateShoppingProfile(result));

            // risk_management
            json.put("risk_management", generateRiskManagement(result));

            // behavior_insights - 新增：行为洞察
            json.put("behavior_insights", generateBehaviorInsights(result));

            // metadata
            json.put("metadata", generateMetadata(result));

            return json.toJSONString();
        }

        private static JSONObject generateUserBaseInfo(DataModel.FinalJoinResult result) {
            JSONObject baseInfo = new JSONObject();

            if (result.userInfo != null) {
                baseInfo.put("birthday", result.userInfo.birthday != null ? result.userInfo.birthday : "");
                baseInfo.put("decade", result.userInfo.ageGroup != null ? result.userInfo.ageGroup : "");

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
                baseInfo.put("zodiac_sign", result.userInfo.constellation != null ? result.userInfo.constellation : "");

                // 星座后面加上"座"
                String constellation = result.userInfo.constellation != null ? result.userInfo.constellation : "";
                if (!constellation.isEmpty() && !constellation.endsWith("座")) {
                    constellation = constellation + "座";
                }
                baseInfo.put("zodiac_sign", constellation);

                // 计算年龄
                int age = calculateAgeFromBirthday(result.userInfo.birthday);
                baseInfo.put("age", age);
                baseInfo.put("membership_level", calculateMembershipLevel(result));

            } else {
                baseInfo.put("birthday", "");
                baseInfo.put("decade", "");
                baseInfo.put("gender", "");
                baseInfo.put("zodiac_sign", "");
                baseInfo.put("age", 0);
                baseInfo.put("membership_level", "unknown");
            }

            return baseInfo;
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

        private static JSONObject generateLoginActivity(DataModel.FinalJoinResult result) {
            JSONObject loginActivity = new JSONObject();

            JSONArray recentLogins = new JSONArray();
            int loginCount = 0;

            DataModel.UserBehaviorHistory behaviorHistory = result.getBehaviorHistory();
            if (behaviorHistory != null && behaviorHistory.getLoginTimes() != null) {
                loginCount = behaviorHistory.getLoginTimes().size();
                // 按时间排序，取最近20次登录
                List<String> sortedLogins = new ArrayList<>(behaviorHistory.getLoginTimes());
                sortedLogins.sort(Collections.reverseOrder());
                int maxLogins = Math.min(sortedLogins.size(), 20);
                for (int i = 0; i < maxLogins; i++) {
                    recentLogins.add(sortedLogins.get(i));
                }
            }

            // 如果没有历史数据，使用当前日志的登录信息
            if (recentLogins.isEmpty() && result.log != null && "login".equals(result.log.logType)) {
                String loginTime = formatTimestamp(result.log.ts);
                recentLogins.add(loginTime);
                loginCount = 1;
            }

            loginActivity.put("recent_logins", recentLogins);
            loginActivity.put("login_count", loginCount);
            loginActivity.put("last_login", recentLogins.size() > 0 ? recentLogins.getString(0) : "");
            loginActivity.put("login_frequency", calculateLoginFrequency(loginCount));

            return loginActivity;
        }

        private static JSONObject generateFinancialProfile(DataModel.FinalJoinResult result) {
            JSONObject financialProfile = new JSONObject();

            // 计算订单数量
            int orderCount = 0;
            double totalAmount = 0.0;

            if (result.comment != null && result.comment.orderId != null) {
                orderCount++;
                if (result.comment.totalAmount != null) {
                    totalAmount = result.comment.totalAmount;
                }
            }
            if (result.log != null && result.log.orderId != null) {
                orderCount++;
            }

            financialProfile.put("consumption_level",
                    result.comment != null && result.comment.consumptionLevel != null ?
                            result.comment.consumptionLevel.toLowerCase() : "unknown");
            financialProfile.put("avg_order_value", totalAmount);
            financialProfile.put("total_orders", orderCount);
            financialProfile.put("lifetime_value", totalAmount > 1500 ? "high_potential" :
                    totalAmount > 500 ? "medium" : "low");

            return financialProfile;
        }

        private static JSONObject generateDeviceAnalysis(Set<DataModel.DeviceInfo> devices) {
            JSONObject deviceAnalysis = new JSONObject();

            if (devices != null && !devices.isEmpty()) {
                Map<String, Integer> brandStats = new HashMap<>();
                Map<String, Integer> platformStats = new HashMap<>();

                for (DataModel.DeviceInfo device : devices) {
                    brandStats.merge(device.brand, 1, Integer::sum);
                    platformStats.merge(device.plat, 1, Integer::sum);
                }

                deviceAnalysis.put("total_devices", devices.size());
                deviceAnalysis.put("brand_distribution", brandStats);
                deviceAnalysis.put("platform_distribution", platformStats);
                deviceAnalysis.put("device_usage_pattern", devices.size() > 1 ? "multi_device" : "single_device");
                deviceAnalysis.put("device_list", generateDeviceList(devices));

            } else {
                deviceAnalysis.put("total_devices", 0);
                deviceAnalysis.put("brand_distribution", new JSONObject());
                deviceAnalysis.put("platform_distribution", new JSONObject());
                deviceAnalysis.put("device_usage_pattern", "unknown");
                deviceAnalysis.put("device_list", new JSONArray());
            }

            return deviceAnalysis;
        }

        private static JSONArray generateDeviceList(Set<DataModel.DeviceInfo> devices) {
            JSONArray deviceList = new JSONArray();
            if (devices != null) {
                for (DataModel.DeviceInfo device : devices) {
                    JSONObject deviceJson = new JSONObject();
                    deviceJson.put("brand", device.brand);
                    deviceJson.put("platform", device.plat);
                    deviceJson.put("platform_version", device.platv);
                    deviceJson.put("software_version", device.softv);
                    deviceJson.put("device_model", device.device);
                    deviceJson.put("userkey", device.userkey);
                    deviceList.add(deviceJson);
                }
            }
            return deviceList;
        }

        private static JSONObject generateSearchBehavior(DataModel.FinalJoinResult result) {
            JSONObject searchBehavior = new JSONObject();

            if (result.getBehaviorHistory() != null && result.getBehaviorHistory().getSearchRecords() != null &&
                    !result.getBehaviorHistory().getSearchRecords().isEmpty()) {

                // 获取所有搜索关键词
                List<String> allKeywords = new ArrayList<>();
                Long lastSearchTime = null;
                int totalSearches = 0;

                for (DataModel.SearchRecord search : result.getBehaviorHistory().getSearchRecords()) {
                    if (search.getKeywords() != null) {
                        allKeywords.addAll(search.getKeywords());
                        totalSearches += search.getKeywords().size();
                    }
                    if (lastSearchTime == null || (search.getTimestamp() != null && search.getTimestamp() > lastSearchTime)) {
                        lastSearchTime = search.getTimestamp();
                    }
                }

                // 去重并限制数量
                Set<String> uniqueKeywords = new LinkedHashSet<>(allKeywords);
                List<String> limitedKeywords = new ArrayList<>(uniqueKeywords);
                if (limitedKeywords.size() > 50) {
                    limitedKeywords = limitedKeywords.subList(0, 50);
                }

                searchBehavior.put("search_keywords", limitedKeywords);
                searchBehavior.put("search_count", totalSearches);
                searchBehavior.put("unique_search_count", uniqueKeywords.size());
                searchBehavior.put("search_sessions", result.getBehaviorHistory().getSearchRecords().size());
                searchBehavior.put("last_search_time", lastSearchTime != null ? formatTimestamp(lastSearchTime) : "");
                searchBehavior.put("search_frequency", calculateSearchFrequency(totalSearches));
                searchBehavior.put("category_analysis", analyzeSearchCategories(allKeywords));

            } else if (result.log != null && "search".equals(result.log.logType) &&
                    result.log.keywords != null && !result.log.keywords.isEmpty()) {

                // 使用当前搜索记录
                searchBehavior.put("search_keywords", result.log.keywords);
                searchBehavior.put("search_count", result.log.keywords.size());
                searchBehavior.put("unique_search_count", new HashSet<>(result.log.keywords).size());
                searchBehavior.put("search_sessions", 1);
                searchBehavior.put("last_search_time", formatTimestamp(result.log.ts));
                searchBehavior.put("search_frequency", "low");
                searchBehavior.put("category_analysis", analyzeSearchCategories(result.log.keywords));

            } else {
                searchBehavior.put("search_keywords", new JSONArray());
                searchBehavior.put("search_count", 0);
                searchBehavior.put("unique_search_count", 0);
                searchBehavior.put("search_sessions", 0);
                searchBehavior.put("last_search_time", "");
                searchBehavior.put("search_frequency", "none");
                searchBehavior.put("category_analysis", new JSONObject());
            }

            return searchBehavior;
        }

        private static JSONObject generateCategoryPreferences(DataModel.FinalJoinResult result) {
            JSONObject categoryPreferences = new JSONObject();

            List<String> preferredCategories = new ArrayList<>();
            double preferenceScore = 0.0;

            if (result.getBehaviorHistory() != null && result.getBehaviorHistory().getSearchRecords() != null) {
                // 基于历史搜索记录分析品类偏好
                for (DataModel.SearchRecord search : result.getBehaviorHistory().getSearchRecords()) {
                    if (search.getKeywords() != null) {
                        for (String keyword : search.getKeywords()) {
                            if ((keyword.contains("瑜伽") || keyword.contains("yoga")) && !preferredCategories.contains("yoga")) {
                                preferredCategories.add("yoga");
                                preferenceScore += 0.3;
                            } else if ((keyword.contains("跑步") || keyword.contains("run")) && !preferredCategories.contains("running")) {
                                preferredCategories.add("running");
                                preferenceScore += 0.3;
                            } else if ((keyword.contains("训练") || keyword.contains("training")) && !preferredCategories.contains("training")) {
                                preferredCategories.add("training");
                                preferenceScore += 0.3;
                            } else if ((keyword.contains("裤") || keyword.contains("pant")) && !preferredCategories.contains("pants")) {
                                preferredCategories.add("pants");
                                preferenceScore += 0.2;
                            } else if ((keyword.contains("上衣") || keyword.contains("shirt")) && !preferredCategories.contains("tops")) {
                                preferredCategories.add("tops");
                                preferenceScore += 0.2;
                            }
                        }
                    }
                }
            }

            categoryPreferences.put("browsed_categories", preferredCategories);
            categoryPreferences.put("purchased_categories", preferredCategories.isEmpty() ?
                    new JSONArray() : preferredCategories);
            categoryPreferences.put("preference_score", Math.min(preferenceScore, 1.0));
            categoryPreferences.put("interest_diversity", preferredCategories.size());

            return categoryPreferences;
        }

        private static JSONObject generateShoppingProfile(DataModel.FinalJoinResult result) {
            JSONObject shoppingProfile = new JSONObject();

            String shoppingGenderPref = "unknown";
            List<String> orderIds = new ArrayList<>();

            if (result.comment != null) {
                // 基于评论内容分析购物性别偏好
                String comment = result.comment.userComment != null ? result.comment.userComment.toLowerCase() : "";
                if (comment.contains("女生") || comment.contains("女士") || comment.contains("女款") || comment.contains("女性")) {
                    shoppingGenderPref = "female";
                } else if (comment.contains("男生") || comment.contains("男士") || comment.contains("男款") || comment.contains("男性")) {
                    shoppingGenderPref = "male";
                }

                if (result.comment.orderId != null) {
                    orderIds.add(result.comment.orderId);
                }
            }

            if (result.log != null && result.log.orderId != null) {
                if (!orderIds.contains(result.log.orderId)) {
                    orderIds.add(result.log.orderId);
                }
            }

            shoppingProfile.put("gender_preference", shoppingGenderPref);
            shoppingProfile.put("order_ids", orderIds);
            shoppingProfile.put("shopping_style", orderIds.size() > 1 ? "frequent" : "occasional");
            shoppingProfile.put("order_count", orderIds.size());

            return shoppingProfile;
        }

        private static JSONObject generateRiskManagement(DataModel.FinalJoinResult result) {
            JSONObject riskManagement = new JSONObject();

            boolean hasSensitiveComment = result.comment != null &&
                    result.comment.triggeredKeyword != null &&
                    !result.comment.triggeredKeyword.isEmpty();

            JSONArray triggeredKeywords = new JSONArray();
            if (hasSensitiveComment) {
                triggeredKeywords.add(result.comment.triggeredKeyword);
            }

            riskManagement.put("sensitive_comment_checked", hasSensitiveComment);
            riskManagement.put("triggered_keywords", triggeredKeywords);
            riskManagement.put("risk_score", hasSensitiveComment ? 0.5 : 0.1);
            riskManagement.put("trust_level", hasSensitiveComment ? "medium" : "high");
            riskManagement.put("ban_days", result.comment != null && result.comment.banDays != null ?
                    result.comment.banDays : 0);

            return riskManagement;
        }

        private static JSONObject generateBehaviorInsights(DataModel.FinalJoinResult result) {
            JSONObject insights = new JSONObject();

            int loginCount = 0;
            int searchCount = 0;
            int deviceCount = result.devices != null ? result.devices.size() : 0;

            if (result.getBehaviorHistory() != null) {
                loginCount = result.getBehaviorHistory().getLoginTimes().size();
                searchCount = result.getBehaviorHistory().getSearchRecords().size();
            }

            // 用户活跃度分析
            String activityLevel = "low";
            if (loginCount >= 10 || searchCount >= 20) activityLevel = "high";
            else if (loginCount >= 5 || searchCount >= 10) activityLevel = "medium";

            // 用户价值分析
            String userValue = "low";
            if (result.comment != null && result.comment.totalAmount != null) {
                if (result.comment.totalAmount > 1000) userValue = "high";
                else if (result.comment.totalAmount > 500) userValue = "medium";
            }

            insights.put("activity_level", activityLevel);
            insights.put("user_value", userValue);
            insights.put("engagement_score", calculateEngagementScore(loginCount, searchCount, deviceCount));
            insights.put("preference_stability", "medium"); // 可根据历史行为计算稳定性
            insights.put("shopping_archetype", determineShoppingArchetype(result));

            return insights;
        }

        private static JSONObject generateMetadata(DataModel.FinalJoinResult result) {
            JSONObject metadata = new JSONObject();

            metadata.put("ds", result.comment != null && result.comment.ds != null ?
                    result.comment.ds : "");
            metadata.put("ts", result.comment != null && result.comment.ts != null ?
                    String.valueOf(result.comment.ts) :
                    (result.log != null && result.log.ts != null ?
                            String.valueOf(result.log.ts) :
                            String.valueOf(System.currentTimeMillis())));
            metadata.put("profile_version", "2.0");
            metadata.put("data_sources", getDataSources(result));
            metadata.put("update_type", result.getBehaviorHistory() != null &&
                    (!result.getBehaviorHistory().getLoginTimes().isEmpty() || !result.getBehaviorHistory().getSearchRecords().isEmpty()) ?
                    "batch_daily" : "realtime");

            return metadata;
        }

        // 辅助方法
        private static String calculateLoginFrequency(int loginCount) {
            if (loginCount >= 50) return "very_high";
            else if (loginCount >= 20) return "high";
            else if (loginCount >= 10) return "medium";
            else if (loginCount >= 5) return "low";
            else return "very_low";
        }

        private static String calculateSearchFrequency(int searchCount) {
            if (searchCount >= 100) return "very_high";
            else if (searchCount >= 50) return "high";
            else if (searchCount >= 20) return "medium";
            else if (searchCount >= 5) return "low";
            else return "very_low";
        }

        private static double calculateEngagementScore(int loginCount, int searchCount, int deviceCount) {
            double score = (loginCount * 0.3) + (searchCount * 0.5) + (deviceCount * 0.2);
            return Math.min(score / 10.0, 1.0);
        }

        private static String determineShoppingArchetype(DataModel.FinalJoinResult result) {
            if (result.getBehaviorHistory() == null) return "new_user";

            int searchCount = result.getBehaviorHistory().getSearchRecords().size();
            int loginCount = result.getBehaviorHistory().getLoginTimes().size();

            if (searchCount > 10) return "researcher";
            else if (loginCount > 5) return "loyal_customer";
            else if (result.comment != null && result.comment.totalAmount > 1000) return "big_spender";
            else return "casual_shopper";
        }

        private static JSONArray getDataSources(DataModel.FinalJoinResult result) {
            JSONArray sources = new JSONArray();
            if (result.comment != null) sources.add("comment");
            if (result.log != null) sources.add("log");
            if (result.userInfo != null) sources.add("user_info");
            if (result.getBehaviorHistory() != null &&
                    (!result.getBehaviorHistory().getLoginTimes().isEmpty() || !result.getBehaviorHistory().getSearchRecords().isEmpty())) {
                sources.add("behavior_history");
            }
            return sources;
        }

        private static JSONObject analyzeSearchCategories(List<String> keywords) {
            JSONObject analysis = new JSONObject();

            int pantsCount = 0;
            int topsCount = 0;
            int shoesCount = 0;
            int yogaCount = 0;
            int runningCount = 0;
            int trainingCount = 0;
            int othersCount = 0;

            for (String keyword : keywords) {
                if (keyword.contains("裤") || keyword.contains("pant")) {
                    pantsCount++;
                } else if (keyword.contains("上衣") || keyword.contains("shirt") || keyword.contains("top")) {
                    topsCount++;
                } else if (keyword.contains("鞋") || keyword.contains("shoe")) {
                    shoesCount++;
                } else if (keyword.contains("瑜伽") || keyword.contains("yoga")) {
                    yogaCount++;
                } else if (keyword.contains("跑步") || keyword.contains("run")) {
                    runningCount++;
                } else if (keyword.contains("训练") || keyword.contains("training")) {
                    trainingCount++;
                } else {
                    othersCount++;
                }
            }

            analysis.put("pants", pantsCount);
            analysis.put("tops", topsCount);
            analysis.put("shoes", shoesCount);
            analysis.put("yoga", yogaCount);
            analysis.put("running", runningCount);
            analysis.put("training", trainingCount);
            analysis.put("others", othersCount);

            return analysis;
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