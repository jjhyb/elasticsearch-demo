package com.yibo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @author: huangyibo
 * @Date: 2019/8/22 22:19
 * @Description:
 *
 * 过滤查询
 */
public class Test5 {

    public static void main(String[] args) throws IOException {
        //查询名称包含手机的，并且品牌为小米的记录
        //1、连接rest接口
        HttpHost http = new HttpHost("127.0.0.1",9200,"http");
        RestClientBuilder restClientBuilder = RestClient.builder(http);//rest构建器
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);//高级客户端对象(连接)
        //2、封装查询请求
        SearchRequest searchRequest = new SearchRequest("sku");//查询请求对象
        searchRequest.types("doc");//设置查询类型
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//查询源构建器

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();//布尔查询构建器

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("brandName", "小米");
        boolQueryBuilder.filter(termQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);
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
