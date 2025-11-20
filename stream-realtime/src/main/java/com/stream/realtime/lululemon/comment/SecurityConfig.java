package com.stream.realtime.lululemon.comment;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 敏感词检测配置
 */
public class SecurityConfig {
    private static final Map<Character, Object> WORD_TREE = new ConcurrentHashMap<Character, Object>();
    private static final Map<String, String> WORD_LEVELS = new ConcurrentHashMap<String, String>();
    private static final boolean USE_IK = true;

    // P1级别关键词模式
    private static final List<Pattern> P1_PATTERNS = new ArrayList<>();

    // P2级别关键词
    private static final List<String> P2_KEYWORDS = new ArrayList<>();

    // 封禁配置
    public static final Map<String, Integer> BAN_DAYS = new HashMap<String, Integer>();
    static {
        BAN_DAYS.put("P0", 365);
        BAN_DAYS.put("P1", 60);
        BAN_DAYS.put("P2", 0);  // P2 级别不触发封禁
    }

    static {
        // 加载 P1 和 P2 敏感词
        try {
            loadSensitiveWordsFromFile("D:\\idea\\daima\\zg6\\stream-bda-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\comment\\p1-sensitive-words.txt", "P1");
            loadSensitiveWordsFromFile("D:\\idea\\daima\\zg6\\stream-bda-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\comment\\p2-negative-words.txt", "P2");
        } catch (Exception e) {
            System.err.println("加载敏感词文件失败: " + e.getMessage());
        }
    }

    public static void loadSensitiveWordsFromFile(String filePath, String level) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            List<String> words = new ArrayList<String>();
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    words.add(line);
                    addWord(line, level);
                }
            }
            System.out.println("加载 " + words.size() + " 个" + level + "敏感词: " + words);
        } catch (Exception e) {
            System.err.println("加载敏感词文件失败: " + e.getMessage());
        }
    }

    public static void addWord(String word, String level) {
        Map<Character, Object> current = WORD_TREE;
        for (int i = 0; i < word.length(); i++) {
            char c = word.charAt(i);
            if (!current.containsKey(c)) {
                current.put(c, new HashMap<Character, Object>());
            }
            current = (Map<Character, Object>) current.get(c);
        }
        current.put('$', level);
        WORD_LEVELS.put(word, level);
    }

    private static SensitiveResult traditionalDetect(String text) {
        List<String> foundWords = new ArrayList<String>();
        String processed = text;
        String maxLevel = "CLEAN";

        for (int i = 0; i < text.length(); i++) {
            Map<Character, Object> current = WORD_TREE;
            for (int j = i; j < text.length(); j++) {
                char c = text.charAt(j);
                if (!current.containsKey(c)) break;

                current = (Map<Character, Object>) current.get(c);
                if (current.containsKey('$')) {
                    String word = text.substring(i, j + 1);
                    String level = (String) current.get('$');
                    String wordWithLevel = word + "(" + level + ")";

                    if (!foundWords.contains(wordWithLevel)) {
                        foundWords.add(wordWithLevel);
                    }

                    String replacement = "***";
                    if ("P1".equals(level)) {
                        replacement = "**";
                    } else if ("P2".equals(level)) {
                        replacement = "*";
                    }
                    processed = processed.replace(word, replacement);

                    if (getLevelWeight(level) > getLevelWeight(maxLevel)) {
                        maxLevel = level;
                    }
                }
            }
        }

        return new SensitiveResult(!foundWords.isEmpty(), maxLevel, processed, foundWords);
    }


    public static void loadP0WordsFromFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            List<String> words = new ArrayList<String>();
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    words.add(line);
                    addWord(line, "P0");
                }
            }
            System.out.println("加载 " + words.size() + " 个P0敏感词: " + words);
        } catch (Exception e) {
            System.err.println("加载P0敏感词文件失败: " + e.getMessage());
        }
    }

    public static SensitiveResult detect(String text) {
        if (text == null || text.trim().isEmpty()) {
            return new SensitiveResult(false, "CLEAN", text, new ArrayList<String>());
        }

        if (USE_IK) {
            return ikDetect(text);
        } else {
            return traditionalDetect(text);
        }
    }

    private static SensitiveResult ikDetect(String text) {
        List<String> allFoundWords = new ArrayList<String>();
        String processed = text;
        String finalLevel = "CLEAN";

        List<String> segments = segmentText(text);
        System.out.println("IK分词结果: " + segments);

        // 方法1: 直接全文匹配（优先）
        SensitiveResult fullTextResult = fullTextDetect(text);
        if (fullTextResult.isSensitive) {
            return fullTextResult;
        }

        // 方法2: 组合词检测
        SensitiveResult combinedResult = detectCombinedWords(segments, text);
        if (combinedResult.isSensitive) {
            return combinedResult;
        }

        // 方法3: 原有的单字检测（作为后备）
        return traditionalSingleWordDetect(segments, text);
    }

    private static SensitiveResult fullTextDetect(String text) {
        List<String> foundWords = new ArrayList<String>();
        String processed = text;
        String maxLevel = "CLEAN";

        for (Map.Entry<String, String> entry : WORD_LEVELS.entrySet()) {
            String word = entry.getKey();
            String level = entry.getValue();

            if (text.contains(word)) {
                foundWords.add(word + "(" + level + ")");
                if (!"P2".equals(level)) {  // P2 级别不进行替换
                    String replacement = getReplacementByLevel(level);
                    processed = processed.replace(word, replacement);
                }

                if (getLevelWeight(level) > getLevelWeight(maxLevel)) {
                    maxLevel = level;
                }
            }
        }

        return new SensitiveResult(!foundWords.isEmpty(), maxLevel, processed, foundWords);
    }

    private static SensitiveResult detectCombinedWords(List<String> segments, String originalText) {
        List<String> foundWords = new ArrayList<String>();
        String processed = originalText;
        String maxLevel = "CLEAN";

        for (int n = 3; n >= 2; n--) {
            for (int i = 0; i <= segments.size() - n; i++) {
                StringBuilder combined = new StringBuilder();
                for (int j = 0; j < n; j++) {
                    combined.append(segments.get(i + j));
                }
                String combinedWord = combined.toString();

                if (WORD_LEVELS.containsKey(combinedWord)) {
                    String level = WORD_LEVELS.get(combinedWord);
                    foundWords.add(combinedWord + "(" + level + ")");
                    if (!"P2".equals(level)) {  // P2 级别不进行替换
                        String replacement = getReplacementByLevel(level);
                        processed = processed.replace(combinedWord, replacement);
                    }

                    if (getLevelWeight(level) > getLevelWeight(maxLevel)) {
                        maxLevel = level;
                    }
                }
            }
        }

        return new SensitiveResult(!foundWords.isEmpty(), maxLevel, processed, foundWords);
    }

    private static SensitiveResult traditionalSingleWordDetect(List<String> segments, String text) {
        List<String> foundWords = new ArrayList<String>();
        String processed = text;
        String maxLevel = "CLEAN";

        for (String segment : segments) {
            if (WORD_LEVELS.containsKey(segment)) {
                String level = WORD_LEVELS.get(segment);
                foundWords.add(segment + "(" + level + ")");
                if (!"P2".equals(level)) {  // P2 级别不进行替换
                    String replacement = getReplacementByLevel(level);
                    processed = processed.replace(segment, replacement);
                }

                if (getLevelWeight(level) > getLevelWeight(maxLevel)) {
                    maxLevel = level;
                }
            }
        }

        return new SensitiveResult(!foundWords.isEmpty(), maxLevel, processed, foundWords);
    }

    private static String getReplacementByLevel(String level) {
        switch (level) {
            case "P0": return "***";
            case "P1": return "**";
            default: return "*";
        }
    }

    private static List<String> segmentText(String text) {
        List<String> segments = new ArrayList<String>();
        try {
            StringReader reader = new StringReader(text);
            IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
            Lexeme lexeme;
            while ((lexeme = ikSegmenter.next()) != null) {
                segments.add(lexeme.getLexemeText());
            }
        } catch (Exception e) {
            System.err.println("IK分词失败，使用字符分词");
            for (int i = 0; i < text.length(); i++) {
                segments.add(String.valueOf(text.charAt(i)));
            }
        }
        return segments;
    }

    private static int getLevelWeight(String level) {
        switch (level) {
            case "P0": return 3;
            case "P1": return 2;
            case "P2": return 1;
            default: return 0;
        }
    }

    public static class SensitiveResult {
        public boolean isSensitive;
        public String level;
        public String processedText;
        public List<String> foundWords;

        private static final Map<String, String> ACTION_MAP = new HashMap<String, String>();
        static {
            ACTION_MAP.put("P0", "BAN_365D_AND_SHIELD");
            ACTION_MAP.put("P1", "BAN_60D_AND_SHIELD");
            ACTION_MAP.put("P2", "NONE");  // P2 级别不触发任何操作
        }

        public SensitiveResult(boolean isSensitive, String level, String processedText, List<String> foundWords) {
            this.isSensitive = isSensitive;
            this.level = level;
            this.processedText = processedText;
            this.foundWords = foundWords;
        }

        public int getBanDays() {
            Integer days = BAN_DAYS.get(level);
            return days != null ? days : 0;
        }

        public String getAction() {
            String action = ACTION_MAP.get(level);
            return action != null ? action : "NONE";
        }
    }
}