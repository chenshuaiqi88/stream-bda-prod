package com.stream.realtime.lululemon.Two_stream_join.Final_version;

import java.io.Serializable;
import java.util.*;

/**
 * 数据模型定义类
 */
public class DataModel {

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

        public String getGroupKey() {
            return (brand + "_" + plat).toLowerCase();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DeviceInfo that = (DeviceInfo) obj;
            return brand.equalsIgnoreCase(that.brand) &&
                    plat.equalsIgnoreCase(that.plat);
        }

        @Override
        public int hashCode() {
            return (brand.toLowerCase() + "_" + plat.toLowerCase()).hashCode();
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
     * Log 数据模型
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
        public List<String> keywords;  // 新增字段

        public LogData(String orderId, String userId, String productId, String logId, Long ts,
                       String ipAddress, DeviceInfo deviceInfo, String logType, List<String> keywords) {
            this.orderId = orderId;
            this.userId = userId;
            this.productId = productId;
            this.logId = logId;
            this.ts = ts;
            this.ipAddress = ipAddress;
            this.deviceInfo = deviceInfo;
            this.logType = logType;
            this.keywords = keywords;
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
     * Comment-Log 连接结果模型
     */
    public static class CommentLogJoinResult {
        public CommentData comment;
        public LogData log;
        public String userId;
        public Long joinTime;
        public Set<DeviceInfo> devices;

        public CommentLogJoinResult(CommentData comment, LogData log, Set<DeviceInfo> devices) {
            this.comment = comment;
            this.log = log;
            this.userId = comment != null ? comment.userId : (log != null ? log.userId : null);
            this.joinTime = System.currentTimeMillis();
            this.devices = devices;
        }
    }

    /**
     * 用户行为历史记录 - 完全序列化安全
     */
    public static class UserBehaviorHistory implements Serializable {
        private Set<String> loginTimes;
        private List<SearchRecord> searchRecords;
        private Long lastUpdateTime;

        public UserBehaviorHistory() {
            this.loginTimes = new HashSet<>();
            this.searchRecords = new ArrayList<>();
            this.lastUpdateTime = System.currentTimeMillis();
        }

        // 安全的getter方法
        public Set<String> getLoginTimes() {
            if (loginTimes == null) {
                loginTimes = new HashSet<>();
            }
            return loginTimes;
        }

        public List<SearchRecord> getSearchRecords() {
            if (searchRecords == null) {
                searchRecords = new ArrayList<>();
            }
            return searchRecords;
        }

        public Long getLastUpdateTime() {
            if (lastUpdateTime == null) {
                lastUpdateTime = System.currentTimeMillis();
            }
            return lastUpdateTime;
        }

        public void addLoginTime(String loginTime) {
            getLoginTimes().add(loginTime);
            this.lastUpdateTime = System.currentTimeMillis();
        }

        public void addSearchRecord(SearchRecord search) {
            getSearchRecords().add(search);
            // 只保留最近100条搜索记录
            if (getSearchRecords().size() > 100) {
                this.searchRecords = getSearchRecords().subList(getSearchRecords().size() - 100, getSearchRecords().size());
            }
            this.lastUpdateTime = System.currentTimeMillis();
        }

        // 序列化时调用的方法
        private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
            out.defaultWriteObject();
            out.writeObject(getLoginTimes());
            out.writeObject(getSearchRecords());
            out.writeObject(getLastUpdateTime());
        }

        @SuppressWarnings("unchecked")
        private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
            in.defaultReadObject();
            this.loginTimes = (Set<String>) in.readObject();
            this.searchRecords = (List<SearchRecord>) in.readObject();
            this.lastUpdateTime = (Long) in.readObject();
        }
    }

    /**
     * 搜索记录 - 完全序列化安全
     */
    public static class SearchRecord implements Serializable {
        private List<String> keywords;
        private Long timestamp;
        private String logType;

        public SearchRecord(List<String> keywords, Long timestamp, String logType) {
            this.keywords = keywords != null ? new ArrayList<>(keywords) : new ArrayList<>();
            this.timestamp = timestamp;
            this.logType = logType != null ? logType : "";
        }

        public List<String> getKeywords() {
            if (keywords == null) {
                keywords = new ArrayList<>();
            }
            return keywords;
        }

        public Long getTimestamp() {
            return timestamp != null ? timestamp : 0L;
        }

        public String getLogType() {
            return logType != null ? logType : "";
        }

        private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
            out.defaultWriteObject();
            out.writeObject(getKeywords());
            out.writeObject(getTimestamp());
            out.writeObject(getLogType());
        }

        @SuppressWarnings("unchecked")
        private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
            in.defaultReadObject();
            this.keywords = (List<String>) in.readObject();
            this.timestamp = (Long) in.readObject();
            this.logType = (String) in.readObject();
        }
    }



    /**
     * 最终连接结果模型 - 完全序列化安全
     */
    public static class FinalJoinResult implements Serializable {
        public CommentData comment;
        public LogData log;
        public UserInfoData userInfo;
        public Set<DeviceInfo> devices;
        private UserBehaviorHistory behaviorHistory;

        // 原有构造函数
        public FinalJoinResult(CommentData comment, LogData log, UserInfoData userInfo, Set<DeviceInfo> devices) {
            this.comment = comment;
            this.log = log;
            this.userInfo = userInfo;
            this.devices = devices;
            this.behaviorHistory = new UserBehaviorHistory();
        }

        // 新构造函数
        public FinalJoinResult(CommentData comment, LogData log, UserInfoData userInfo,
                               Set<DeviceInfo> devices, UserBehaviorHistory behaviorHistory) {
            this.comment = comment;
            this.log = log;
            this.userInfo = userInfo;
            this.devices = devices;
            this.behaviorHistory = behaviorHistory != null ? behaviorHistory : new UserBehaviorHistory();
        }

        public UserBehaviorHistory getBehaviorHistory() {
            if (behaviorHistory == null) {
                behaviorHistory = new UserBehaviorHistory();
            }
            return behaviorHistory;
        }

        public String toJson() {
            return StreamUtils.JsonUtils.generateFinalResultJson(this);
        }

        private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
            out.defaultWriteObject();
            out.writeObject(comment);
            out.writeObject(log);
            out.writeObject(userInfo);
            out.writeObject(devices);
            out.writeObject(getBehaviorHistory());
        }

        @SuppressWarnings("unchecked")
        private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
            in.defaultReadObject();
            this.comment = (CommentData) in.readObject();
            this.log = (LogData) in.readObject();
            this.userInfo = (UserInfoData) in.readObject();
            this.devices = (Set<DeviceInfo>) in.readObject();
            this.behaviorHistory = (UserBehaviorHistory) in.readObject();
        }
    }


}