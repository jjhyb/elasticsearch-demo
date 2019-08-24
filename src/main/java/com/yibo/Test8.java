package com.yibo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @author: huangyibo
 * @Date: 2019/8/22 22:00
 * @Description:
 *
 * 匹配查询 查询价格区间的商品
 *
 * SearchRequest： 查询请求对象
 * SearchResponse：查询响应对象
 * SearchSourceBuilder：查询源构建器
 * MatchQueryBuilder：匹配查询构建器
 */
public class Test8 {

    public static void main(String[] args) throws IOException {
        //查询商品名称包含手机的记录
        //1、连接rest接口
        HttpHost http = new HttpHost("127.0.0.1",9200,"http");
        RestClientBuilder restClientBuilder = RestClient.builder(http);//rest构建器
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);//高级客户端对象(连接)
        //2、封装查询请求
        SearchRequest searchRequest = new SearchRequest("sku");//查询请求对象
        searchRequest.types("doc");//设置查询类型
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//查询源构建器

        //查询价格区间，get()表示大于等于，lte表示小于等于
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("price").gte("10000").lte("20000");
        searchSourceBuilder.query(rangeQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        //3、获取查询结果
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("查询记录数："+totalHits);
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
        restHighLevelClient.close();
    }
}
