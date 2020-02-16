package com.wintermar.kg

import org.apache.spark.sql.SparkSession
import org.neo4j.spark.Neo4j

object PageRankJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("PageRank")
      .master("local[*]")
      .config("spark.neo4j.bolt.url", "bolt://localhost")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "123456")
//      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    val neo = Neo4j(sparkSession.sparkContext)

    val graphFrame = neo
      .pattern(("Character", "name"), ("INTERACTS", "name"), ("Character", "name"))
      .loadGraphFrame

    val pageRankFrame = graphFrame.pageRank.maxIter(10).run()
    // tol(delta) 当前后两次差值小于delta时停止迭代
    // graphFrame.pageRank.tol(0.1).run()

    val rankVertices = pageRankFrame.vertices
    // weight为pagerank生成的权重字段
    val rankEdges = pageRankFrame.edges.drop("weight")

    // 注册三张视图
    rankVertices.withColumnRenamed("value", "src_name")
      .withColumnRenamed("pagerank", "src_pr")
      .createOrReplaceTempView("src_v")

    rankVertices.withColumnRenamed("value", "dst_name")
      .withColumnRenamed("pagerank", "dst_pr")
      .createOrReplaceTempView("dst_v")

    rankEdges.createOrReplaceTempView("edge")

    val resultFrame = sparkSession
      .sql(" select * from edge " +
      " left join src_v on edge.src=src_v.id " +
      " left join dst_v on edge.dst=dst_v.id ")
      .drop("id")

    resultFrame.show(10)

    sparkSession.close()


  }

}
