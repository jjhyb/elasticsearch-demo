package com.yibo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.Map;

/**
 * @author: huangyibo
 * @Date: 2019/8/22 22:00
 * @Description:
 *
 * 匹配查询 高亮查询
 *
 * SearchRequest： 查询请求对象
 * SearchResponse：查询响应对象
 * SearchSourceBuilder：查询源构建器
 * MatchQueryBuilder：匹配查询构建器
 */
public class Test12 {

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
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name","手机");
        searchSourceBuilder.query(matchQueryBuilder);//query{}部分

        //查询关键字高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("name").preTags("<font style='color:red'>").postTags("</font>");

        searchSourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(searchSourceBuilder);
        //3、获取查询结果
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("查询记录数："+totalHits);
        SearchHit[] hits = searchHits.getHits();
        System.out.println("高亮结果：");
        for (SearchHit hit : hits) {
            //String sourceAsString = hit.getSourceAsString();
            //System.out.println(sourceAsString);

            //获取高亮内容
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("name");
            Text[] fragments = highlightField.fragments();
            String str = fragments[0].toString();
            System.out.println(str);
        }
        restHighLevelClient.close();
    }
}
