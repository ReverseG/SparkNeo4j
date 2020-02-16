package com.wintermar.kg

import org.apache.spark.sql.SparkSession
import org.neo4j.driver.v1.{AuthToken, AuthTokens, GraphDatabase}
import org.neo4j.spark.Neo4j
import org.apache.spark.sql.functions._

object LouvainJob {

  /**
    *
    * 进行社区划分的cypher语句， 将社区写入到节点的community字段
    *
    * CALL algo.louvain("Character", "INTERACTS",
    * {iteration:20, write:True, writeProperty:'community'})
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("PageRank")
      .master("local[*]")
      .config("spark.neo4j.bolt.url", "bolt://localhost")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "123456")
      //      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    val label = "红楼梦"
    val neo = Neo4j(sparkSession.sparkContext)


//    val louvainCypher = " call algo.louvain('红楼梦', '关系', {iteration:20, write:True, writeProperty:'community'}) "

    val labelPropogationCypher = "call algo.labelPropagation('红楼梦','关系',{write:True, writeProperty:'community'})"

    val cypherSession = GraphDatabase.driver("bolt://localhost:7687",
      AuthTokens.basic("neo4j", "123456")).session()
    cypherSession.run(labelPropogationCypher)
    cypherSession.close()


    val queryCypher = "MATCH (n:`红楼梦`)-[r]->(m) " +
      " return ID(n) as src_id, n.name as src_name, n.community as src_community, " +
      " ID(r) as re_id, r.name as re_name, " +
      " ID(m) as dst_name, m.community as dst_community "

    val resultFrame = neo.cypher(queryCypher).loadDataFrame

    resultFrame.groupBy("src_community")
      .count()
      .orderBy(desc("count"))
      .show()


    sparkSession.close()


  }
}
