package com.stream.realtime.lululemon.DbusLogETL;
import java.util.HashMap;
import java.util.Map;
public class CityProvinceMapper {

        private static final Map<String, String> CITY_TO_PROVINCE = new HashMap<>();

        static {
            // ====== 直辖市 ======
            CITY_TO_PROVINCE.put("北京", "北京市");
            CITY_TO_PROVINCE.put("天津", "天津市");
            CITY_TO_PROVINCE.put("上海", "上海市");
            CITY_TO_PROVINCE.put("重庆", "重庆市");

            // ====== 广东省 ======
            String[] gd = {"广州", "深圳", "珠海", "佛山", "东莞", "中山", "惠州", "江门", "汕头", "湛江",
                    "茂名", "肇庆", "梅州", "揭阳", "清远", "阳江", "潮州", "韶关", "汕尾"};
            for (String c : gd) CITY_TO_PROVINCE.put(c, "广东省");

            // ====== 江苏省 ======
            String[] js = {"南京", "苏州", "无锡", "常州", "南通", "扬州", "镇江", "泰州", "盐城",
                    "连云港", "淮安", "宿迁", "徐州"};
            for (String c : js) CITY_TO_PROVINCE.put(c, "江苏省");

            // ====== 浙江省 ======
            String[] zj = {"杭州", "宁波", "温州", "嘉兴", "湖州", "绍兴", "金华", "衢州", "舟山", "台州", "丽水"};
            for (String c : zj) CITY_TO_PROVINCE.put(c, "浙江省");

            // ====== 安徽省 ======
            String[] ah = {"合肥", "芜湖", "蚌埠", "马鞍山", "安庆", "滁州", "宿州", "阜阳", "亳州",
                    "六安", "铜陵", "池州", "宣城", "淮南", "淮北"};
            for (String c : ah) CITY_TO_PROVINCE.put(c, "安徽省");

            // ====== 湖北省 ======
            String[] hb = {"武汉", "黄石", "襄阳", "宜昌", "鄂州", "荆门", "孝感", "荆州", "黄冈",
                    "咸宁", "随州", "恩施"};
            for (String c : hb) CITY_TO_PROVINCE.put(c, "湖北省");

            // ====== 河南省 ======
            String[] hn = {"郑州", "开封", "洛阳", "平顶山", "安阳", "鹤壁", "新乡", "焦作", "濮阳",
                    "许昌", "漯河", "三门峡", "商丘", "周口", "驻马店", "南阳"};
            for (String c : hn) CITY_TO_PROVINCE.put(c, "河南省");

            // ====== 辽宁省 ======
            String[] ln = {"沈阳", "大连", "鞍山", "抚顺", "本溪", "丹东", "锦州", "营口", "阜新",
                    "辽阳", "盘锦", "铁岭", "朝阳", "葫芦岛"};
            for (String c : ln) CITY_TO_PROVINCE.put(c, "辽宁省");

            // ====== 山东省 ======
            String[] sd = {"济南", "青岛", "淄博", "枣庄", "东营", "烟台", "潍坊", "济宁", "泰安",
                    "威海", "日照", "临沂", "德州", "聊城", "滨州", "菏泽"};
            for (String c : sd) CITY_TO_PROVINCE.put(c, "山东省");

            // ====== 四川省 ======
            String[] sc = {"成都", "自贡", "攀枝花", "泸州", "德阳", "绵阳", "广元", "遂宁", "内江",
                    "乐山", "南充", "眉山", "宜宾", "广安", "达州", "雅安", "巴中", "资阳"};
            for (String c : sc) CITY_TO_PROVINCE.put(c, "四川省");

            // ====== 黑龙江省 ======
            String[] hl = {"哈尔滨", "齐齐哈尔", "牡丹江", "佳木斯", "大庆", "鸡西", "鹤岗", "双鸭山", "伊春",
                    "七台河", "黑河", "绥化"};
            for (String c : hl) CITY_TO_PROVINCE.put(c, "黑龙江省");

            // ====== 吉林省 ======
            String[] jl = {"长春", "吉林", "四平", "辽源", "通化", "白山", "松原", "白城"};
            for (String c : jl) CITY_TO_PROVINCE.put(c, "吉林省");

            // ====== 河北省 ======
            String[] hb2 = {"石家庄", "唐山", "秦皇岛", "邯郸", "邢台", "保定", "张家口", "承德",
                    "沧州", "廊坊", "衡水"};
            for (String c : hb2) CITY_TO_PROVINCE.put(c, "河北省");

            // ====== 湖南省 ======
            String[] hn2 = {"长沙", "株洲", "湘潭", "衡阳", "邵阳", "岳阳", "常德", "张家界", "益阳",
                    "郴州", "永州", "怀化", "娄底"};
            for (String c : hn2) CITY_TO_PROVINCE.put(c, "湖南省");

            // ====== 江西省 ======
            String[] jx = {"南昌", "景德镇", "萍乡", "九江", "新余", "鹰潭", "赣州", "吉安", "宜春", "抚州", "上饶"};
            for (String c : jx) CITY_TO_PROVINCE.put(c, "江西省");

            // ====== 陕西省 ======
            String[] sx = {"西安", "铜川", "宝鸡", "咸阳", "渭南", "延安", "汉中", "榆林", "安康", "商洛"};
            for (String c : sx) CITY_TO_PROVINCE.put(c, "陕西省");

            // ====== 云南省 ======
            String[] yn = {"昆明", "曲靖", "玉溪", "保山", "昭通", "丽江", "普洱", "临沧"};
            for (String c : yn) CITY_TO_PROVINCE.put(c, "云南省");

            // ====== 广西壮族自治区 ======
            String[] gx = {"南宁", "柳州", "桂林", "梧州", "北海", "防城港", "钦州", "贵港", "玉林", "百色", "贺州", "河池", "来宾", "崇左"};
            for (String c : gx) CITY_TO_PROVINCE.put(c, "广西壮族自治区");

            // ====== 贵州省 ======
            String[] gz = {"贵阳", "六盘水", "遵义", "安顺", "毕节", "铜仁"};
            for (String c : gz) CITY_TO_PROVINCE.put(c, "贵州省");

            // ====== 福建省 ======
            String[] fj = {"福州", "厦门", "莆田", "三明", "泉州", "漳州", "南平", "龙岩", "宁德"};
            for (String c : fj) CITY_TO_PROVINCE.put(c, "福建省");

            // ====== 海南省 ======
            String[] hn3 = {"海口", "三亚", "三沙", "儋州"};
            for (String c : hn3) CITY_TO_PROVINCE.put(c, "海南省");

            // ====== 甘肃省 ======
            String[] gs = {"兰州", "嘉峪关", "金昌", "白银", "天水", "武威", "张掖", "平凉", "酒泉", "庆阳", "定西", "陇南"};
            for (String c : gs) CITY_TO_PROVINCE.put(c, "甘肃省");

            // ====== 青海省 ======
            String[] qh = {"西宁", "海东"};
            for (String c : qh) CITY_TO_PROVINCE.put(c, "青海省");

            // ====== 新疆维吾尔自治区 ======
            String[] xj = {"乌鲁木齐", "克拉玛依", "吐鲁番", "哈密", "喀什", "阿克苏", "和田", "奎屯"};
            for (String c : xj) CITY_TO_PROVINCE.put(c, "新疆维吾尔自治区");

            // ====== 内蒙古自治区 ======
            String[] nm = {"呼和浩特", "包头", "乌海", "赤峰", "通辽", "鄂尔多斯", "呼伦贝尔", "巴彦淖尔", "乌兰察布"};
            for (String c : nm) CITY_TO_PROVINCE.put(c, "内蒙古自治区");

            // ====== 宁夏回族自治区 ======
            String[] nx = {"银川", "石嘴山", "吴忠", "固原", "中卫"};
            for (String c : nx) CITY_TO_PROVINCE.put(c, "宁夏回族自治区");

            // ====== 西藏自治区 ======
            String[] xz = {"拉萨", "日喀则", "昌都", "山南", "林芝", "那曲"};
            for (String c : xz) CITY_TO_PROVINCE.put(c, "西藏自治区");

            // ====== 香港、澳门、台湾 ======
            CITY_TO_PROVINCE.put("香港", "香港特别行政区");
            CITY_TO_PROVINCE.put("澳门", "澳门特别行政区");
            CITY_TO_PROVINCE.put("台北", "台湾省");
            CITY_TO_PROVINCE.put("高雄", "台湾省");
            CITY_TO_PROVINCE.put("台中", "台湾省");
            CITY_TO_PROVINCE.put("新竹", "台湾省");
            CITY_TO_PROVINCE.put("基隆", "台湾省");
        }

        /***
         * 根据城市名获取所属省份
         */
        public static String[] getProvinceAndCity(String region) {
            String province = "未知省";
            String city = "未知市";

            if (region == null || region.isEmpty()) {
                return new String[]{province, city};
            }

            // 去掉“省”“市”等噪声字符
            String cleanRegion = region
                    .replace("省", "")
                    .replace("市", "")
                    .replace("区", "")
                    .replace("县", "")
                    .trim();

            // 遍历映射表匹配城市关键字
            for (Map.Entry<String, String> entry : CITY_TO_PROVINCE.entrySet()) {
                String cityName = entry.getKey();
                if (cleanRegion.contains(cityName) || cityName.contains(cleanRegion)) {
                    province = entry.getValue();
                    city = cityName + "市";
                    break;
                }
            }

            return new String[]{province, city};
        }



}
