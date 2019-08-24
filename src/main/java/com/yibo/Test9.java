package com.yibo;

import com.alibaba.fastjson.JSON;

import java.util.Map;

/**
 * @author: huangyibo
 * @Date: 2019/8/23 1:38
 * @Description:
 */
public class Test9 {

    public static void main(String[] args) {
        String str = "{'颜色': '紫色', '尺码': '300度'}";
        Map map = JSON.parseObject(str, Map.class);

        map.forEach((key,value) -> {
            System.out.println(key +": " + value);
        });

    }
}
