package com.yibo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: huangyibo
 * @Date: 2019/8/22 21:38
 * @Description:
 *
 * 插入单条数据：
 * HttpHost  :  url地址封装
 * RestClientBuilder： rest客户端构建器
 * RestHighLevelClient： rest高级客户端
 * IndexRequest： 新增或修改请求
 * IndexResponse：新增或修改的响应结果
 *
 *
 * 如果ID不存在则新增，如果ID存在则修改。
 */
public class Test1 {

    public static void main(String[] args) throws IOException {
        //插入单条数据：
        //1、连接rest接口
        HttpHost http = new HttpHost("127.0.0.1",9200,"http");
        RestClientBuilder restClientBuilder = RestClient.builder(http);//rest构建器
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);//高级客户端对象(连接)
        //2、封装请求对象
        IndexRequest indexRequest = new IndexRequest("sku","doc","3");
        Map skuMap = new HashMap();
        skuMap.put("name","华为p30pro");
        skuMap.put("brandName","华为");
        skuMap.put("categoryName","手机");
        skuMap.put("price",480000);
        skuMap.put("createTime","2019-04-16");
        skuMap.put("saleNum",101021);
        skuMap.put("commentNum",10102321);
        Map specMap = new HashMap();
        specMap.put("网络制式","移动4G");
        specMap.put("屏幕尺寸","5");
        skuMap.put("spec",specMap);
        indexRequest.source(skuMap);

        //3、获取执行结果
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        int status = indexResponse.status().getStatus();
        System.out.println(status);
        restHighLevelClient.close();
    }
}
