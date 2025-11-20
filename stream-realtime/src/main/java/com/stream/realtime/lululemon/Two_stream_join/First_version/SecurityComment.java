package com.stream.realtime.lululemon.Two_stream_join.First_version;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.annotation.JSONField;

/**
 * 安全评论数据模型
 */
class SecurityComment {
    @JSONField(name = "order_id")
    public String orderId;
    
    @JSONField(name = "user_id")
    public String userId;
    
    @JSONField(name = "sensitive_level")
    public String sensitiveLevel;
    
    @JSONField(name = "is_blocked")
    public Boolean isBlocked;
    
    @JSONField(name = "blacklist_duration_days")
    public Integer banDays;
    
    @JSONField(name = "triggered_keyword")
    public String triggeredKeyword;
    
    @JSONField(name = "total_amount")
    public Double totalAmount;
    
    @JSONField(name = "consumption_level")
    public String consumptionLevel;
    
    @JSONField(name = "user_comment")
    public String userComment;
    
    @JSONField(name = "ds")
    public String ds;
    
    @JSONField(name = "ts")
    public Long ts;
    
    @JSONField(name = "product_id")
    public String productId;

    // 新增字段 - 从 log 中获取的 IP
    public String ipAddress;

    public static SecurityComment fromJson(String json) {
        try {
            JSONObject jsonObject = JSONObject.parseObject(json);
            SecurityComment comment = new SecurityComment();
            comment.orderId = jsonObject.getString("order_id");
            comment.userId = jsonObject.getString("user_id");
            comment.productId = jsonObject.getString("product_id");
            comment.ds = jsonObject.getString("ds");
            comment.ts = jsonObject.getLong("ts");
            comment.isBlocked = jsonObject.getBoolean("is_blocked");
            comment.userComment = jsonObject.getString("user_comment");
            comment.sensitiveLevel = jsonObject.getString("sensitive_level");
            comment.banDays = jsonObject.getInteger("blacklist_duration_days");
            comment.triggeredKeyword = jsonObject.getString("triggered_keyword");
            comment.totalAmount = jsonObject.getDouble("total_amount");
            comment.consumptionLevel = jsonObject.getString("consumption_level");
            return comment;
        } catch (Exception e) {
            System.err.println("解析 SecurityComment 失败: " + json);
            throw new RuntimeException("解析 SecurityComment 失败", e);
        }
    }
    
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("order_id", orderId);
        json.put("user_id", userId);
        json.put("product_id", productId);
        json.put("ds", ds);
        json.put("ts", ts);
        json.put("is_insulting", isBlocked);
        json.put("user_comment", userComment);
        json.put("db", "realtime_v3");
        json.put("schema", "dbo");
        json.put("table", "orders_portrait_stream2");
        json.put("sensitive_level", sensitiveLevel);
        json.put("is_blocked", isBlocked);
        json.put("blacklist_duration_days", banDays);
        json.put("triggered_keyword", triggeredKeyword);
        json.put("total_amount", totalAmount);
        json.put("consumption_level", consumptionLevel);
        
        // 添加 IP 地址
        if (ipAddress != null) {
            json.put("ip_address", ipAddress);
        }
        
        return json;
    }
    
    public String toJsonString() {
        return toJson().toJSONString();
    }
}

/**
 * 实时日志数据模型
 */
class RealtimeLog {
    @JSONField(name = "order_id")
    public String orderId;
    
    @JSONField(name = "user_id")
    public String userId;
    
    @JSONField(name = "product_id")
    public String productId;
    
    @JSONField(name = "log_id")
    public String logId;
    
    @JSONField(name = "ts")
    public Long ts;
    
    private String ipAddress;

    public String getIpAddress() {
        return ipAddress;
    }

    public static RealtimeLog fromJson(String json) {
        try {
            JSONObject jsonObject = JSONObject.parseObject(json);
            RealtimeLog log = new RealtimeLog();
            log.orderId = jsonObject.getString("order_id");
            log.userId = jsonObject.getString("user_id");
            log.productId = jsonObject.getString("product_id");
            log.logId = jsonObject.getString("log_id");
            log.ts = jsonObject.getLong("ts");
            
            // 手动解析嵌套的 gis.ip
            if (jsonObject.containsKey("gis")) {
                JSONObject gis = jsonObject.getJSONObject("gis");
                if (gis != null && gis.containsKey("ip")) {
                    log.ipAddress = gis.getString("ip");
                }
            }
            
            return log;
        } catch (Exception e) {
            System.err.println("解析 RealtimeLog 失败: " + json);
            throw new RuntimeException("解析 RealtimeLog 失败", e);
        }
    }
}

/**
 * Join 结果数据模型
 */
class JoinedResult {
    public SecurityComment enrichedComment;
    public Long joinTime;
    
    public JoinedResult(SecurityComment enrichedComment) {
        this.enrichedComment = enrichedComment;
        this.joinTime = System.currentTimeMillis();
    }
    
    public String toJson() {
        return enrichedComment.toJsonString();
    }
}