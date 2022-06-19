package org.mdp.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordering.Implicits.seqDerivedOrdering

/**
 * Scala object to run Spark job that computes some results over
 * the dataset resulting of ClosestRelativeWithMostSightings.
 *
 * Contains only the main method.
 *
 * @author Ismael Correa, Ricardo Espinoza, Benjamín Bustos
 */
object Results {

  /**
   * Main method
   *
   * @params args Command line args. First element is path of the text files to use as input
   *         of job, second element is path in which to write lvlToCount RDD and third element is
   *         path in which to write noRelatives RDD.
   */
  def main(args: Array[String]): Unit = {

    // Initialises a Spark Context with name of application and default settings.
    val sc = new SparkContext(new SparkConf().setAppName("Generate results"))

    // Load the result of Closest Relative With Most Sightings Spark Job, splitting by '\t'.
    val resultDataset = sc.textFile(args(0) + "part-*").map(line => line.split("\t").toList)

    /* For each species, map the level of closest relative to the name of said level. If there was a species
     * with no relatives, generate here a tuple ("None", "None").
     */
    val lvlToNameLvl = resultDataset.map(l => (if (l(2).nonEmpty)
                                               (l(2).split(" ").toList(0), l(2).split(" ").toList(2))
                                               else ("None", "None")))

    // Count, for each taxonomic level, the number of species that found its closest relative in said level.
    val lvlToCount = lvlToNameLvl.map(p => (p._1, 1)).reduceByKey((x, y) => x + y)

    // Search if there was species that found no relatives in the dataset.
    val noRelatives = resultDataset.filter(l => (l(2) == ""))

    // Write lvlToCount
    lvlToCount.coalesce(1).saveAsTextFile(args(1))

    // Write noRelatives
    noRelatives.coalesce(1).saveAsTextFile(args(2))
  }
}
