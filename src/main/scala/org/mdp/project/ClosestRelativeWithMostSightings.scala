package org.mdp.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Scala object to run Spark job that computes closest relative with most
 * sightings for each species.
 *
 * Contains only the main method.
 *
 * @author Ismael Correa, Ricardo Espinoza, Benjamín Bustos
 */
object ClosestRelativeWithMostSightings {

  /**
   * Main method
   *
   * @params args Command line args. First element is path of the text file to use as input
   *         of job, and second element is path in which to write result.
   */
  def main(args: Array[String]) {

    // Initialises a Spark Context with name of application and default settings.
    val sc = new SparkContext(new SparkConf().setAppName("Closest Relative With Most Sightings"))

    /* Load the first RDD from source text file path (args(0)), and split each line on character
     * '\t', the result is of type RDD[List[String]]. let l be a List containing the result of
     * splitting a line of the dataset (each line is an occurrence or sighting):
     *
     *    - l(0) -> kingdom
     *    - l(1) -> phylum
     *    - l(2) -> class
     *    - l(3) -> order
     *    - l(4) -> family
     *    - l(5) -> genus
     *    - l(6) -> species
     */
    val originalDataset: RDD[List[String]] = sc.textFile(args(0)).map(line => line.split("\t").toList)

    // Map each sighting to a one, so next those ones can be added aggregating by species.
    val aggregatedBySighting: RDD[(List[String], Int)] = originalDataset.map(l => (l, 1))

    /* Previous RDD gets agreggated by key (species), adding all ones for each species.
     * The result is of type RDD[(List[String], Int)], where each species gets mapped to the number
     * of times it occurs in the dataset (number of sightings).
     */
    val aggregatedBySpecies: RDD[(List[String], Int)] = aggregatedBySighting.reduceByKey((x, y) => x + y)

    /* Previous RDD gets mapped to an RDD of type RDD[(List[String], (String, Int))], which corresponds
     * to pairs whose content is ([kingdom, ..., genus], (species, speciesCount)).
     */
    val genusToSpeciesCount: RDD[(List[String], (String, Int))] = aggregatedBySpecies.map(p => (p._1.slice(0, 6), (p._1(6), p._2)))

    /* First of all, the previous RDD gets joined with itself, resulting in an RDD of type
     * RDD[(List[String], ((String, Int), (String, Int)))], then that RDD gets mapped to an RDD containing
     * the following information: ([kingdom, ..., genus, species], (species2, countOfSpecies2)), so each species gets
     * mapped to all other species in the genus and their occurrences in the dataset. Then, that RDD gets grouped by key,
     * resulting in an RDD of type RDD[List[String], Iterable[(String, Int)]], where the iterable
     * contains all tuples of (species2, countOfSpecies2) that belong to the same genus as the species whose information is in
     * the first component of the RDD. Finally, that RDD gets mapped to an RDD of type RDD[(List[String], (String, String, Int))],
     * where the second component is a 3-tuple (speciesWithMaxCountGenusLvl, genus - nameGenus, count). That is, the resulting RDD
     * maps each list [kingdom, ..., genus, species] to the species with most occurrences in the same genus if said genus
     * doesnt contain only one species (the species in the list), the string "genus" followed by the name of the genus, and
     * the count of the species with most occurrences in the genus. Now, if the genus contained only the species being
     * inspected (the species in the list), then the 3-tuple is ("", "", 0).
     */
    val speciesToClosesRelativeGenusLvl: RDD[(List[String], (String, String, Int))] = genusToSpeciesCount.join(genusToSpeciesCount)
      .map {
        case (l, ((s1, _), (s2, c2))) => (l ++ List(s1), (s2, c2))
      }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.foldRight(("", "", 0)) {
          case ((s1, c1), (s2, lvl, c2)) => if (s1 != l(6) && c1 > c2) (s1, s"genus - ${l(5)}", c1) else (s2, lvl, c2)
        })
      }

    /* This transformation is similar to the previous with only some differences: First the RDD speciesToClosesRelativeGenusLvl
     * gets mapped to an RDD of type RDD[List[String], (String, (String, String, Int))] where each row contains
     * the following info: ([kingdom, ..., family], (species1, (speciesWithMaxCountGenusLvl, genus - nameGenus, count))),
     * where the species named species1 corresponds to the info in [kingdom, ..., family]. Then the previous RDD gets
     * grouped by key, resulting in an RDD of type RDD[List[String], Iterable[(String, (String, String, Int))]], where
     * the iterable contains all species in the family given by the List, with its corresponding species with most
     * occurrences in the same genus (if there is one). Finally, that RDD gets mapped to another RDD of type
     * RDD[List[String], Iterable[(String, (String, String, Int))]], where the species with most occurrences in the family gets
     * computed via a fold, and if the species in the first component of each tuple of type (String, (String, String, Int))
     * maps to a 3-tuple ("", "", 0) (that happens if it had no relatives at the genus lvl), then that 3-tuple gets
     * replaced with a new 3-tuple named max which contains (speciesWithMaxCountFamilyLvl, family - nameFamily, count).
     * Now, it can be the case that the 3-tuple max is again of the form ("", "", 0), which happens if the family working
     * as key had only one species (the first component of the values of type (String, (String, String, Int))); if that
     * is the case, it will remain as ("", "", 0), and in the next iteration it will be replaced with the species at the
     * next taxonomic level with most sightings (if it exist), and so on. The final flatMapValues is used to map
     * each row of type (List[String], Iterable[(String, (String, String, Int))]) to multiple rows of type
     * (List[String], (String, (String, String, Int))), flatting up the iterable.
     */
    val familyToSpeciesToClosestRelativeFamilyLvl: RDD[(List[String], (String, (String, String, Int)))] = speciesToClosesRelativeGenusLvl.map {
      case (l, (s, lvl, count)) => (l.slice(0, 5), (l(6), (s, lvl, count)))
    }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
            (p1._2._1, s"family - ${l(4)}", p1._2._3)
          else p2)
          if (p._2 == ("", "", 0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    /* Transformation is analogous to the previous one but at the order level.
     */
    val orderToSpeciesToClosestRelativeOrderLvl = familyToSpeciesToClosestRelativeFamilyLvl.map {
      case (l, speciesToClosestRelative) => (l.slice(0, 4), speciesToClosestRelative)
    }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
            (p1._2._1, s"order - ${l(3)}", p1._2._3)
          else p2)
          if (p._2 == ("", "", 0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    /* Transformation is analogous to the previous one but at the class level.
     */
    val classToSpeciesToClosestRelativeClassLvl = orderToSpeciesToClosestRelativeOrderLvl.map {
      case (l, speciesToClosestRelative) => (l.slice(0, 3), speciesToClosestRelative)
    }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
            (p1._2._1, s"class - ${l(2)}", p1._2._3)
          else p2)
          if (p._2 == ("", "", 0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    /* Transformation is analogous to the previous one but at the phylum level.
     */
    val phylumToSpeciesToClosestRelativePhylumLvl = classToSpeciesToClosestRelativeClassLvl.map {
      case (l, speciesToClosestRelative) => (l.slice(0, 2), speciesToClosestRelative)
    }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
            (p1._2._1, s"phylum - ${l(1)}", p1._2._3)
          else p2)
          if (p._2 == ("", "", 0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    /* Transformation is analogous to the previous one but at the kingdom level.
     */
    val kingdomToSpeciesToClosestRelativeKingdomLvl = phylumToSpeciesToClosestRelativePhylumLvl.map {
      case (l, speciesToClosestRelative) => (l.slice(0, 1), speciesToClosestRelative)
    }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
            (p1._2._1, s"kingdom - ${l(0)}", p1._2._3)
          else p2)
          if (p._2 == ("", "", 0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    /* Finally the result is the previous RDD where each row is formatted as a simple string. The information
     * contained in each row of the result is:
     * species \t closestRelativeWithMostOccurrences \t levelOfClosesRelative \t countOfClosestRelativeWithMostSightings
     */
    val result = kingdomToSpeciesToClosestRelativeKingdomLvl.map {
      case (_, (s, (closestRelative, lvl, count))) => s"${s}\t${closestRelative}\t${lvl}\t${count}"
    }

    // Result gets written.
    result.saveAsTextFile(args(1))
  }
}

