package com.yibo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: huangyibo
 * @Date: 2019/8/22 21:54
 * @Description:
 *
 * 批处理请求
 * BulkRequest：  批量请求（用于增删改操作）
 * BulkResponse：批量请求（用于增删改操作）
 *
 */
public class Test2 {

    public static void main(String[] args) throws IOException {
        //1、连接rest接口
        HttpHost http = new HttpHost("127.0.0.1",9200,"http");
        RestClientBuilder restClientBuilder = RestClient.builder(http);//rest构建器
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);//高级客户端对象(连接)
        //2、封装请求对象
        BulkRequest bulkRequest = new BulkRequest();//BulkRequest可以封装多个IndexRequest
        IndexRequest indexRequest = new IndexRequest("sku","doc","5");
        Map skuMap = new HashMap();
        skuMap.put("name","华为meta20pro");
        skuMap.put("brandName","华为");
        skuMap.put("categoryName","手机");
        skuMap.put("price",580000);
        skuMap.put("createTime","2019-04-16");
        skuMap.put("saleNum",101021);
        skuMap.put("commentNum",10102321);
        Map specMap = new HashMap();
        specMap.put("网络制式","移动4G");
        specMap.put("屏幕尺寸","6");
        skuMap.put("spec",specMap);
        indexRequest.source(skuMap);
        bulkRequest.add(indexRequest);//可以多次添加

        //3、获取执行结果
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        int status = bulkResponse.status().getStatus();
        System.out.println(status);
        String message = bulkResponse.buildFailureMessage();
        System.out.println(message);
        restHighLevelClient.close();
    }
}
