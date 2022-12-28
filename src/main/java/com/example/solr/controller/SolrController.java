package com.example.solr.controller;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/solr")
public class SolrController {
    @Autowired
    private SolrClient client;

    @PostMapping("/add")
    public Object add( @RequestParam String name, @RequestParam String age) throws SolrServerException, IOException {
        String idStr = String.valueOf(System.currentTimeMillis());

        SolrInputDocument document = new SolrInputDocument();
        document.setField("id", idStr);
        document.setField("name", name);
        document.setField("age",age);

        // 把文档对象写入索引库
        client.add(document);//如果配置文件中没有指定core，这个方法的第一个参数就需要指定core名称,比如client.add("core", doc);
        client.commit();//如果配置文件中没有指定core，这个方法的第一个参数就需要指定core名称client.commit("core");
        return idStr;
    }

    @PostMapping("/update")
    public Object updateDocument(@RequestParam String idStr,
                                 @RequestParam String name,
                                 @RequestParam String age) throws Exception{
        SolrInputDocument document = new SolrInputDocument();
        document.setField("id", idStr);
        document.setField("name", name);
        document.setField("age",age);
        // 把文档对象写入索引库
        client.add(document);
        // 提交
        client.commit();
        return document;
    }

    /**
     * 查询文档
     * @param condition 条件
     * @param collection 连接文件夹 默 core
     * @param pageStart 分页起始 默 1
     * @param pageEnd   分页结束 默 10
     * @return
     * @throws Exception
     */
    @GetMapping("/query")
    public Object queryDocument(@RequestParam String condition,
                                @RequestParam String collection,
                                @RequestParam Integer pageStart,
                                @RequestParam Integer pageEnd) throws Exception {
        // 创建一个查询条件
        SolrQuery solrQuery = new SolrQuery();
        // 设置查询条件
        solrQuery.setQuery("name:小黄人");
        // 设置分页
        solrQuery.setStart(pageStart);
        solrQuery.setRows(pageEnd);
        // 执行查询
        QueryResponse query = client.query(solrQuery);
//        QueryResponse query = client.query(collection,solrQuery);
        // 取查询结果
        SolrDocumentList solrDocumentList = query.getResults();

        System.out.println("总记录数：" + solrDocumentList.getNumFound());

        for (SolrDocument sd : solrDocumentList) {
            System.out.println(sd.get("id"));
            System.out.println(sd.get("name"));
            System.out.println(sd.get("age"));
        }
        return solrDocumentList;
    }

    @PostMapping("/delete")
    public Object deteleDocument(@RequestParam String collection,
                                 @RequestParam String idStr) throws Exception {
        // 根据id删除
        UpdateResponse response = client.deleteById(collection, idStr);
        // 根据条件删除
        // httpSolrServer.deleteByQuery("");
        // 提交
        client.commit(collection);

        return response;
    }


    /**
     *全量或增量更新，可以结合定时任务做定时全量或增量更新
     * @throws SolrServerException
     * @throws IOException
     */
    @RequestMapping(value = "updateSolrData",method = RequestMethod.PUT)
    public void updateSolrData() throws SolrServerException, IOException {
        //创建一个查询对象
        SolrQuery solrQuery = new SolrQuery();

        //增量更新全部完成；注意这里entity的值为solr-data-config.xml里entity标签里的name值
        final String SOLR_DELTA_PARAM = "/dataimport?command=delta-import&entity=order_info&clean=false&commit=true";
        //全量更新全部完成
        final String SOLR_FULL_PARAM = "/dataimport?command=full-import&entity=order_info&clean=true&commit=true";
        //设置更新方式
        solrQuery.setRequestHandler(SOLR_DELTA_PARAM);

        // 执行查询
        QueryResponse query = client.query("core",solrQuery);

        //提交
        client.commit("core");

    }


}
