package org.mdp.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordering.Implicits.seqDerivedOrdering

object Results {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Generate results"))

    val resultDataset = sc.textFile(args(0) + "part-*").map(line => line.split("\t").toList)

    val lvlToNameLvl = resultDataset.map(l => (if (l(2).nonEmpty)
                                               (l(2).split(" ").toList(0), l(2).split(" ").toList(2))
                                               else ("None", "None")))

    val lvlToCount = lvlToNameLvl.map(p => (p._1, 1)).reduceByKey((x, y) => x + y)

    val lvlNameToCount = lvlToNameLvl.map {
        case (lvl, lvlName) => ((lvl, lvlName), 1)
      }
      .reduceByKey((x, y) => x + y)

    val orderedLvlNameToCount = lvlNameToCount.sortBy(_._2, ascending = false)

    val noRelatives = resultDataset.filter(l => (l(2) == ""))

    lvlToCount.coalesce(1).saveAsTextFile(args(1))

    orderedLvlNameToCount.coalesce(1).saveAsTextFile(args(2))

    noRelatives.coalesce(1).saveAsTextFile(args(3))
  }
}
