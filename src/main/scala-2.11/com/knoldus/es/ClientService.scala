package com.knoldus.es

import java.io.{File, PrintWriter}
import java.net.InetAddress

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.deletebyquery.{DeleteByQueryAction, DeleteByQueryResponse}
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin

import scala.io.Source

class ClientService {


  def client():Client = {

    val client: Client = TransportClient.builder().addPlugin(classOf[DeleteByQueryPlugin]).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
    client
  }

  def add(name: String, scale: String, amount: Int, id: String): IndexResponse = {
    client.prepareIndex("employee", "salary", id)
      .setSource(XContentFactory.jsonBuilder()
        .startObject()
        .field("name", name)
        .field("scale", scale)
        .field("amount", amount)
        .endObject()
      )
      .get()
  }


  def delete(id: Int) = {
    val delQuery = QueryBuilders.boolQuery.must(QueryBuilders.termsQuery("_id", id))
    val delResponse: DeleteByQueryResponse = DeleteByQueryAction.INSTANCE
      .newRequestBuilder(client)
      .setIndices("employee")
      .setQuery(delQuery).execute().actionGet()
    delResponse
  }

  def getCount = {
    client.prepareSearch("employee").execute().actionGet().getHits.totalHits()
  }

  def searchAll = {
    client.prepareSearch("employee").execute().actionGet()
  }

  def search(query: String, value: String) = {
    client.prepareSearch("employee").setTypes("salary").setQuery(QueryBuilders.termQuery(s"$query", s"$value")).execute().actionGet()

  }

  def update(id: String, field: String, value: Int): UpdateResponse = {
    client.prepareUpdate("employee", "salary", id).setDoc(field, value).execute().actionGet()
  }

  def readFromJson(file:String)={
    val fileData=Source.fromFile(file).getLines()
    val result=client.prepareBulk()
    fileData.toList.map{jsonvar=>result.add(client.prepareIndex("socialmedia","twitter").setSource(jsonvar))}
    result.execute().actionGet()
  }

  def writeToJson()={
    val fileData=client.prepareSearch("socialmedia").setTypes("twitter").execute().get()
    val result=new PrintWriter(new File("/home/knoldus_pallavi/output.json"))
    result.write(fileData.toString)
  }
}
