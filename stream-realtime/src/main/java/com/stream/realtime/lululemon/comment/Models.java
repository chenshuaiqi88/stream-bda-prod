package com.stream.realtime.lululemon.comment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据模型类
 */
class CommentRecord {
    public String userId;
    public String orderId;
    public String productId;    // 新增product_id字段
    public String commentContent;
    public String commentTime;  // ds字段
    public Long timestamp;      // ts字段
    public Double totalAmount;

    // 修改为7个参数的构造函数，对应7个字段
    public CommentRecord(String userId, String orderId, String productId, String commentContent,
                         String commentTime, Long timestamp, Double totalAmount) {
        this.userId = userId;
        this.orderId = orderId;
        this.productId = productId;
        this.commentContent = commentContent;
        this.commentTime = commentTime;
        this.timestamp = timestamp;
        this.totalAmount = totalAmount;
    }
}

class UserInfo {
    public String userId;
    public String userName;
    public Integer age;
    public List<String> loginTime;
    public String consumptionLevel;
    public Map<String, Object> deviceInfo;

    public UserInfo(String userId, String userName, Integer age, List<String> loginTime,
                    String consumptionLevel, Map<String, Object> deviceInfo) {
        this.userId = userId;
        this.userName = userName;
        this.age = age;
        this.loginTime = loginTime;
        this.consumptionLevel = consumptionLevel;
        this.deviceInfo = deviceInfo;
    }
}

class ProcessResult {
    public String userId;
    public String orderId;
    public String originalComment;
    public String processedComment;
    public Boolean isSensitive;
    public String sensitiveLevel;
    public List<String> detectedWords;
    public Integer banDays;
    public String action;
    public Long processTime;
    public UserInfo userInfo;
    public Double totalAmount;
    public String commentTime;
    public String consumptionLevel; // 新增消费级别字段

    // 修改构造函数，添加 consumptionLevel 参数
    public ProcessResult(String userId, String orderId,
                         String originalComment, String processedComment,
                         Boolean isSensitive, String sensitiveLevel,
                         List<String> detectedWords, Integer banDays,
                         String action, UserInfo userInfo, Double totalAmount,
                         String commentTime, String consumptionLevel) {
        this.userId = userId;
        this.orderId = orderId;
        this.originalComment = originalComment;
        this.processedComment = processedComment;
        this.isSensitive = isSensitive;
        this.sensitiveLevel = sensitiveLevel;
        this.detectedWords = detectedWords;
        this.banDays = banDays;
        this.action = action;
        this.processTime = System.currentTimeMillis();
        this.userInfo = userInfo;
        this.totalAmount = totalAmount;
        this.commentTime = commentTime;
        this.consumptionLevel = consumptionLevel; // 新增赋值
    }

    public String toJson() {
        // 优先选择最高级别的敏感词作为 triggered_keyword
        String triggeredKeyword = "";
        if (detectedWords != null && !detectedWords.isEmpty() && !"P2".equals(sensitiveLevel)) {
            // 查找P0级别的词
            for (String word : detectedWords) {
                if (word.contains("(P0)")) {
                    triggeredKeyword = word.substring(0, word.indexOf("("));
                    break;
                }
            }

            // 如果没有P0，查找P1级别的词
            if (triggeredKeyword.isEmpty()) {
                for (String word : detectedWords) {
                    if (word.contains("(P1)")) {
                        triggeredKeyword = word.substring(0, word.indexOf("("));
                        break;
                    }
                }
            }

            // 如果还没有，取第一个词（但P2级别不显示triggered_keyword）
            if (triggeredKeyword.isEmpty() && !"P2".equals(sensitiveLevel)) {
                String firstWord = detectedWords.get(0);
                if (firstWord.contains("(")) {
                    triggeredKeyword = firstWord.substring(0, firstWord.indexOf("("));
                } else {
                    triggeredKeyword = firstWord;
                }
            }
        }

        // 处理金额格式
        String amountStr;
        if (totalAmount != null) {
            if (totalAmount == totalAmount.intValue()) {
                amountStr = String.valueOf(totalAmount.intValue());
            } else {
                amountStr = String.format("%.2f", totalAmount);
            }
        } else {
            amountStr = "0";
        }

        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"order_id\":\"").append(escapeJson(orderId != null ? orderId : "")).append("\",");
        json.append("\"user_id\":\"").append(escapeJson(userId != null ? userId : "")).append("\",");
        json.append("\"product_id\":\"").append(escapeJson("your_product_id_value")).append("\",");
        json.append("\"ds\":\"").append(escapeJson(commentTime != null ? commentTime : "")).append("\",");
        json.append("\"ts\":\"").append(processTime).append("\",");
        json.append("\"is_insulting\":").append(isSensitive).append(",");
        json.append("\"user_comment\":\"").append(escapeJson(originalComment)).append("\",");
        json.append("\"db\":\"realtime_v3\",");
        json.append("\"schema\":\"dbo\",");
        json.append("\"table\":\"orders_portrait_stream2\",");
        json.append("\"sensitive_level\":\"").append(sensitiveLevel).append("\",");
        json.append("\"is_blocked\":").append(isSensitive).append(",");
        json.append("\"blacklist_duration_days\":").append(banDays).append(",");

        // 只有当不是P2级别且有triggered_keyword时才添加该字段
        if (!"P2".equals(sensitiveLevel) && !triggeredKeyword.isEmpty()) {
            json.append("\"triggered_keyword\":\"").append(escapeJson(triggeredKeyword)).append("\",");
        }

        // 移除了 keyword_source 字段
        json.append("\"total_amount\":").append(amountStr).append(",");
        json.append("\"consumption_level\":\"").append(consumptionLevel != null ? consumptionLevel : "LOW").append("\"");
        json.append("}");

        return json.toString();
    }

    /**
     * 获取敏感词的级别权重
     */
    private int getKeywordLevelWeight(String keywordWithLevel) {
        if (keywordWithLevel.contains("(P0)")) {
            return 3;
        } else if (keywordWithLevel.contains("(P1)")) {
            return 2;
        } else if (keywordWithLevel.contains("(P2)")) {
            return 1;
        } else {
            return 0;
        }
    }

    // escapeJson 方法保持不变
    private String escapeJson(String text) {
        if (text == null) return "";
        return text.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}