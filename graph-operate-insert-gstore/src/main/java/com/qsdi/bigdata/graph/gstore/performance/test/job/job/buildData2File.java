package com.qsdi.bigdata.graph.gstore.performance.test.job.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qsdi.bigdata.graph.gstore.performance.test.job.conf.BaseConf;
import com.qsdi.bigdata.graph.gstore.performance.test.job.kafka.GraphKafkaProducer;
import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiVertex;
import com.qsdi.bigdata.multi.graph.common.id.GraphID;
import com.qsdi.bigdata.multi.graph.common.id.IdGenerator;
import com.qsdi.bigdata.multi.graph.common.id.SnowflakeIdGenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 构造数据
 *
 * @author lijie0203 2024/7/17 18:09
 */
public class buildData2File {
//        graphKafkaConsumer = new GraphKafkaConsumer(kafkaProperties);
    /**
     * Created by LiuJianZe on 2024/5/28
     * 随机姓名
     **/
    // 姓氏池
    private static final String XING = "赵钱孙李周吴郑王冯陈楮卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜";
    // 名字池
    private static final String MING = "三四五六七八建国强国富民少年强则中国强泰骞旭平秦齐良清明泉德东锋玮昊惟文峥杉哲坤灵熙岩泽商晨纶秦恒征风竽磊炫灿承彬奕宗浩泽永茂万信畅裕清深信霖圣晨博罗泽若宇军祯卓秉雄君启庆星珅南源泽灏风思杉泽明舒志林灏泽元浚梓顺远铭锵滨义茂彦自昊承文云诺若翰凯泽鸣哲慕鸿瑞若皓润浦宇嘉和亦茂忆狄";

    public static String getName() {
        // 获取姓氏池的随机下标并随机获取一个姓氏
        char xing = XING.charAt((int) (Math.random() * XING.length()));

        // 创建一个可扩容字符串
        StringBuilder userName = new StringBuilder().append(xing);

        // 随机生成1或2，决定名字长度
        int mingLength = 1 + (int) (Math.random() * 2);
        int mingPoolLength = MING.length();

        for (int i = 0; i < mingLength; i++) {
            // 获取名字池的随机下标并随机获取一个名字字符并拼接
            char ming = MING.charAt((int) (Math.random() * mingPoolLength));
            userName.append(ming);
        }

        return userName.toString();
    }

    public static void main(String[] args) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("D:\\tmp\\gstore\\性能测试\\vertex_data200W"))) {
            for (int i = 0; i < 2000000; i++) {

                Map<String, Object> properties = new HashMap<>();
                String generatorId = SnowflakeIdGenerator.defaultInstance().generate().asString();
                int labelId = new Random().nextInt(99);
                String idString = labelId + ":" + generatorId.substring(5);

                properties.put("archiveNo", generatorId);
                properties.put("gmsfhm", generatorId);
                properties.put("qsdiAccessTime", new Date());
                properties.put("lxdh", System.currentTimeMillis() / 100);
                properties.put("xm", getName());
                properties.put("mzdm", new Random().nextInt(56));
                properties.put("xbdm", new Random().nextInt(3));
                properties.put("faceLibId", new Random().nextInt(10000));
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("id", idString);
                jsonObject.put("properties", properties);
                bufferedWriter.write(JSON.toJSONString(jsonObject));
                bufferedWriter.newLine();

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
