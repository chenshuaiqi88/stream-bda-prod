package com.stream.realtime.lululemon.comment;

import java.util.*;

/**
 * 用户服务类
 */
public class UserService {
    private static final Map<String, UserInfo> USER_DB = new HashMap<String, UserInfo>();

    static {
        // 模拟用户数据
        Map<String, Object> deviceInfo1 = new HashMap<String, Object>();
        deviceInfo1.put("device", "iPhone");
        deviceInfo1.put("os", "iOS15");

        Map<String, Object> deviceInfo2 = new HashMap<String, Object>();
        deviceInfo2.put("device", "Android");
        deviceInfo2.put("os", "Android12");

        Map<String, Object> deviceInfo3 = new HashMap<String, Object>();
        deviceInfo3.put("device", "Huawei");
        deviceInfo3.put("os", "EMUI11");

        Map<String, Object> deviceInfo4 = new HashMap<String, Object>();
        deviceInfo4.put("device", "Xiaomi");
        deviceInfo4.put("os", "MIUI13");

        USER_DB.put("b2acee3e5bf34cbda52866ff5b4ebb79",
                new UserInfo("b2acee3e5bf34cbda52866ff5b4ebb79", "user28", 28,
                        Arrays.asList("2025-09-12 13:24:41", "2025-10-15 09:12:33"),
                        "MEDIUM", deviceInfo1));

        USER_DB.put("2c0b7dedaa824ac582fff9d8fb2505e6",
                new UserInfo("2c0b7dedaa824ac582fff9d8fb2505e6", "user35", 35,
                        Arrays.asList("2025-08-01 10:11:12"),
                        "HIGH", deviceInfo2));

        USER_DB.put("c8c611f1c6a645eb8a0595a1a1dc709f",
                new UserInfo("c8c611f1c6a645eb8a0595a1a1dc709f", "user42", 42,
                        Arrays.asList("2025-10-20 08:30:15"),
                        "LOW", deviceInfo3));

        USER_DB.put("56d93afd2ee64b77a42dbc91a2e90cd6",
                new UserInfo("56d93afd2ee64b77a42dbc91a2e90cd6", "user25", 25,
                        Arrays.asList("2025-11-01 14:20:30"),
                        "HIGH", deviceInfo4));
    }

    public static UserInfo getUser(String userId) {
        UserInfo user = USER_DB.get(userId);
        if (user == null) {
            user = new UserInfo(userId, "unknown", 0,
                    new ArrayList<String>(), "UNKNOWN", new HashMap<String, Object>());
        }
        return user;
    }
}