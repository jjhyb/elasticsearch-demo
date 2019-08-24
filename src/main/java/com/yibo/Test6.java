package com.yibo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author: huangyibo
 * @Date: 2019/8/22 22:19
 * @Description:
 *
 * 分组（聚合）查询
 * AggregationBuilders：聚合构建器工厂
 * TermsAggregationBuilder：词条聚合构建器
 * Aggregations：分组结果封装
 * Terms.Bucket： 桶
 */
public class Test6 {

    public static void main(String[] args) throws IOException {
        //按商品分类分组查询，求出每个分类的文档数
        //1、连接rest接口
        HttpHost http = new HttpHost("127.0.0.1",9200,"http");
        RestClientBuilder restClientBuilder = RestClient.builder(http);//rest构建器
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);//高级客户端对象(连接)
        //2、封装查询请求
        SearchRequest searchRequest = new SearchRequest("sku");//查询请求对象
        searchRequest.types("doc");//设置查询类型
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//查询源构建器

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("sku_category").field("categoryName");
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchSourceBuilder.size(0);//不输出详细结果集，只输出分组结果
        searchRequest.source(searchSourceBuilder);

        //3、获取查询结果
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        /*SearchHits searchHits = searchResponse.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("查询记录数："+totalHits);
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }*/
        Aggregations aggregations = searchResponse.getAggregations();
        Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
        Terms terms = (Terms)aggregationMap.get("sku_category");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKeyAsString()+": "+bucket.getDocCount());
        }
        restHighLevelClient.close();
    }
}
