package com.wintermar.kg



import org.apache.spark.sql.SparkSession
import org.neo4j.spark._
import org.apache.spark.graphx._
import org.neo4j.driver.internal.InternalNode

object SparkNeo4jJob {

  def main(args: Array[String]): Unit = {
    // sparkConf -> sparkContext -> SQLContext
    //    val conf = new SparkConf().
    //      setAppName("neo4j")
    //      .set("spark.neo4j.bolt.url", "bolt://localhost:7687")
    //      .set("spark.neo4j.bolt.user", "neo4j")
    //      .set("spark.neo4j,bolt.password", "123456")
    //    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)

    // sparkSession -> sparkContext
    val sparkSession = SparkSession.builder()
      .appName("neo4j")
      .master("local[*]")
      .config("spark.neo4j.bolt.url", "bolt://127.0.0.1:7687")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "123456")
      .getOrCreate();

    val neo = Neo4j(sparkSession.sparkContext);

    // 读取关系
    val relQuery = "MATCH (n:Character)-[r]->(m:Character) RETURN id(n) as source, id(m) as target, type(r) as value"
    val graph: Graph[String, String] = neo.rels(relQuery).loadGraph
    graph.edges.foreach(print(_))
    
    

    // 读取节点
    val nodeQuery = "MATCH (n:Character) return n"
    val rdd = neo.cypher(nodeQuery).loadRowRdd
    val characterRdd = rdd.map(row=>{
      val map = row.get(0).asInstanceOf[InternalNode]
      new Character(map.id(), map.get("name").asString())
    })
    characterRdd.foreach(print(_))

    // 读取关系和节点
    val graphFrame = neo
      .pattern(("红楼梦","name"), ("关系","weight"), ("红楼梦","name"))
      .partitions(1)
      .loadGraphFrame
    val pageRankFrame = graphFrame.pageRank.maxIter(10).run()         
    val ranks = pageRankFrame.vertices
    ranks.foreach(println(_))

    


    val nodeQuery2 = "MATCH (m:Person) return ID(m) as id, m.name as name"
    // 读取RDD
//    val nodes = neo.cypher(nodeQuery2).rows(10).loadRowRdd
//    nodes.foreach(row => {
//      println(row.getLong(0))
//      println(row.getString(1))
//    })
    // 因为RDD的操作不好操作，所以改为load dataframe
    val nodeDF = neo.cypher(nodeQuery2).loadDataFrame
    nodeDF.show(10)
    
    

  }


}

case class Character(
                    val id:Long,
                    val name:String
                    )