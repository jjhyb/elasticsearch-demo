package com.yibo;

import com.alibaba.fastjson.JSON;
import com.yibo.domain.Sku;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
 * 需求将goods库中的sku表中的数据全部导入elasticsearch中
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:applicationContext-dao.xml")
public class Test7 {

    @Autowired
    private SkuMapper skuMapper;

    @Test
    public void importElasticSearch() throws IOException {

        //1、连接rest接口
        HttpHost http = new HttpHost("127.0.0.1",9200,"http");
        RestClientBuilder restClientBuilder = RestClient.builder(http);//rest构建器
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);//高级客户端对象(连接)
        //2、封装请求对象
        BulkRequest bulkRequest = new BulkRequest();//BulkRequest可以封装多个IndexRequest

        Example example = new Example(Sku.class);
        example.createCriteria().andEqualTo("status","1");
        example.setOrderByClause("id");
        List<Sku> skuList = skuMapper.selectByExample(example);
        //这里批量导入数据到elasticsearch中，一次导入9万条数据，ES直接OOM异常了，这里每次用45000条进行批量导入成功
        for(int i=45000;i<skuList.size();i++) {
            Sku sku = skuList.get(i);
            IndexRequest indexRequest = new IndexRequest("sku","doc",sku.getId());
            Map skuMap = new HashMap();
            skuMap.put("name",sku.getName());
            skuMap.put("brandName",sku.getBrandName());
            skuMap.put("categoryName",sku.getCategoryName());
            skuMap.put("price",sku.getPrice());
            skuMap.put("createTime",sku.getCreateTime());
            skuMap.put("updateTime",sku.getUpdateTime());
            skuMap.put("image",sku.getImage());
            skuMap.put("saleNum",sku.getSaleNum());
            skuMap.put("commentNum",sku.getCommentNum());
            skuMap.put("spuId",sku.getSpuId());
            skuMap.put("categoryId",sku.getCategoryId());
            skuMap.put("weight",sku.getWeight());
            String spec = sku.getSpec();
            Map map = JSON.parseObject(spec, Map.class);
            Map specMap = new HashMap();
            if(!CollectionUtils.isEmpty(map)){
                map.forEach((key,value) -> {
                    specMap.put(key,value);
                });
            }
            skuMap.put("spec",specMap);
            indexRequest.source(skuMap);
            bulkRequest.add(indexRequest);//可以多次添加
        }
        //3、获取执行结果
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        int status = bulkResponse.status().getStatus();
        System.out.println(status);
        String message = bulkResponse.buildFailureMessage();
        System.out.println(message);
        restHighLevelClient.close();

    }

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
