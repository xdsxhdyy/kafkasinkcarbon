package com.dat.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Author luoyu
 * @create 2019/12/9 15:47
 */
public class Pg2PgJson {
    public static void main(String[] args) throws Exception {
        //读取文件 设定格式:ip|port|username|password|schema|table|target_table|splitPk|pk_column|columns|add_column
        BufferedReader br = new BufferedReader(new FileReader("E:/libo2/Desktop/test/haiwai.txt"));
        String line = null;
        //读每行数据
        while ((line = br.readLine()) != null) {
            //文件内容放入数组
            String[] info = line.split("\\|");
            String target_table = info[6];
            //拼装全量Json信息
            String str = toJson(line.trim(), "full");
            //全量抽数json
            BufferedWriter bw = new BufferedWriter(new FileWriter("E:/libo2/Desktop/test/ods/" + target_table + ".json"));
            //写入全量文件
            bw.write(str);
            bw.flush();
            bw.close();
            //拼装增量Json信息
            String str1 = toJson(line.trim(), "add");
            //增量抽数json
            BufferedWriter bw1 = new BufferedWriter(new FileWriter("E:/libo2/Desktop/test/stg/" + target_table + ".json"));
            //写入增量文件
            bw1.write(str1);
            bw1.flush();
            bw1.close();
        }
    }

    /**
     * @description Json信息
     * @author libo2
     * @date 2019/7/31 20:09
     */
    public static String toJson(String line, String flag) {
        //文件内容放入数组
        String[] info = line.split("\\|");
        String ip = info[0];
        String port = info[1];
        String username = info[2];
        String password = info[3];
        String schema = info[4];
        String tableName = info[5];
        String target_table = info[6];
        String splitPk = info[7];
        if ("null".equals(splitPk.toLowerCase())) {
            splitPk = null;
        }
        String pkColumn = info[8];
        if ("null".equals(pkColumn.toLowerCase())) {
            pkColumn = null;
        }
        String allColumns = info[9];
        String addColumn = null;//增量字段
        if (info.length > 10) {
            addColumn = info[10];
        }
        //从最里层往外扩
        //********reader部分********
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:postgresql://" + ip + ":" + port + "/");
        sb.append(schema);
        sb.append("?autoReconnect=true");//开启自动重连，防止连接时间短超时
        Map<String, Object> m1 = new HashMap<String, Object>();
        List<String> rjdbcList = new ArrayList<String>();
        rjdbcList.add(sb.toString());
        m1.put("jdbcUrl", rjdbcList);//源jdbc信息
        List<String> rtableList = new ArrayList<String>();
        rtableList.add(tableName);
        m1.put("table", rtableList);//源表名
        List<Object> rconnList = new ArrayList<Object>();
        rconnList.add(m1);
        Map<String, Object> mm1 = new HashMap<String, Object>();
        mm1.put("column", allColumns.split(","));//源各个字段
        mm1.put("connection", rconnList);//源连接信息
        mm1.put("username", username);//源用户名
        mm1.put("password", password);//源密码
        mm1.put("where", "1=1");//源条件
        if (StringUtils.isNoneEmpty(splitPk)) {
            mm1.put("splitPk", splitPk);//源分割字段
        }
        if ("add".equals(flag)) {//增量条件
            if (StringUtils.isNoneEmpty(addColumn)) {
                String[] addCol = addColumn.split(";");
                String add1 = addCol[0];
                String add2 = addCol[1];
                mm1.put("where", String.format("%s>=now() - interval '2 day' or %s>=now() - interval '2 day'", add1, add2));
            }
        }
        Map<String, Object> mmm1 = new HashMap<String, Object>();
        mmm1.put("name", "postgresqlreader");//源数据源
        mmm1.put("parameter", mm1);//源参数
        Map<String, Object> mmmm1 = new HashMap<String, Object>();
        mmmm1.put("reader", mmm1);

        //********writer部分********
        Map<String, Object> m2 = new HashMap<String, Object>();
        m2.put("jdbcUrl", "jdbc:postgresql://192.168.66.94:5434/dw");//目标jdbc信息
        List<String> wtableList = new ArrayList<String>();
        String ods_table = target_table;//全量表
        List<Object> wconnList = new ArrayList<Object>();
        wconnList.add(m2);
        Map<String, Object> mm2 = new HashMap<String, Object>();
        mm2.put("column", allColumns.split(","));//目标各个字段
        mm2.put("connection", wconnList);//目标连接信息
        mm2.put("username", "aaaaa");//目标用户名
        mm2.put("password", "123456");//目标密码
        String stg_table = target_table + "_stg";//增量临时表
        if ("add".equals(flag)) {
            wtableList.add(stg_table);
            mm2.put("preSql", new String[]{String.format("truncate table %s;", stg_table)});//执行语句之前操作
            if (StringUtils.isNoneEmpty(pkColumn) && StringUtils.isNoneEmpty(addColumn)) {//执行语句之后操作
                mm2.put("postSql", new String[]{String.format("delete from %s a where exists (select 1 from %s b where a.%s=b.%s);insert into %s select * from %s;", ods_table, stg_table, pkColumn, pkColumn, ods_table, stg_table)});
            } else {
                mm2.put("postSql", new String[]{String.format("delete from %s;insert into %s select * from %s;", ods_table, ods_table, stg_table)});
            }
        } else {
            wtableList.add(ods_table);
            mm2.put("preSql", new String[]{String.format("truncate table %s;", ods_table)});//执行语句之前操作
        }
        m2.put("table", wtableList);//目标表名
        Map<String, Object> mmm2 = new HashMap<String, Object>();
        mmm2.put("name", "postgresqlwriter");//目标数据源
        mmm2.put("parameter", mm2);//目标参数
        mmmm1.put("writer", mmm2);

        List<Object> contentList = new ArrayList<Object>();
        contentList.add(mmmm1);
        Map<String, Object> m3 = new HashMap<String, Object>();
        m3.put("content", contentList);

        Map<String, Object> m4 = new HashMap<String, Object>();
        m4.put("channel", "10");
        Map<String, Object> mm4 = new HashMap<String, Object>();
        mm4.put("speed", m4);
        m3.put("setting", mm4);

        Map<String, Object> m5 = new HashMap<String, Object>();
        m5.put("job", m3);
        String str = JSON.toJSONString(m5);
        //JSON格式化
        JSONObject object = JSONObject.parseObject(str);
        str = JSON.toJSONString(object, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat);
        return str;
    }

}
