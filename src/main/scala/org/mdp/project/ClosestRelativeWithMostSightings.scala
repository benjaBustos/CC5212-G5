package org.mdp.project

import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author ${user.name}
 */
object ClosestRelativeWithMostSightings {
  
  def main(args : Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Closest Relative With Most Sightings"))

    val originalDataset = sc.textFile(args(0)).map(line => line.split("\t").toList)

    val aggregatedBySighting = originalDataset.map(l => (l, 1))

    val aggregatedBySpecies = aggregatedBySighting.reduceByKey((x, y) => x + y)

    val genusToSpeciesCount = aggregatedBySpecies.map(p => (p._1.slice(0, 6), (p._1(6), p._2)))

    val speciesToClosesRelativeGenusLvl = genusToSpeciesCount.join(genusToSpeciesCount)
      .map {
        case (l, ((s1, _), (s2, c2))) => (l ++ List(s1), (s2, c2))
      }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.foldRight(("", "", 0)) {
          case ((s1, c1), (s2, lvl, c2)) => if (s1 != l(6) && c1 > c2)  (s1, s"genus - ${l(5)}", c1) else (s2, lvl, c2)
        })
      }

    val familyToSpeciesToClosestRelativeFamilyLvl = speciesToClosesRelativeGenusLvl.map {
        case (l, (s, lvl, count)) => (l.slice(0, 5), (l(6), (s, lvl, count)))
      }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
                                                               (p1._2._1, s"family - ${l(4)}", p1._2._3)
                                                            else p2)
          if (p._2 == ("","",0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    val orderToSpeciesToClosestRelativeOrderLvl = familyToSpeciesToClosestRelativeFamilyLvl.map {
        case (l, speciesToClosestRelative) => (l.slice(0, 4), speciesToClosestRelative)
      }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
                                                               (p1._2._1, s"order - ${l(3)}", p1._2._3)
                                                            else p2)
          if (p._2 == ("","",0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    val classToSpeciesToClosestRelativeClassLvl = orderToSpeciesToClosestRelativeOrderLvl.map {
      case (l, speciesToClosestRelative) => (l.slice(0, 3), speciesToClosestRelative)
    }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
            (p1._2._1, s"class - ${l(2)}", p1._2._3)
          else p2)
          if (p._2 == ("","",0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    val phylumToSpeciesToClosestRelativePhylumLvl = classToSpeciesToClosestRelativeClassLvl.map {
      case (l, speciesToClosestRelative) => (l.slice(0, 2), speciesToClosestRelative)
    }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
            (p1._2._1, s"phylum - ${l(1)}", p1._2._3)
          else p2)
          if (p._2 == ("","",0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    val kingdomToSpeciesToClosestRelativeKingdomLvl = phylumToSpeciesToClosestRelativePhylumLvl.map {
      case (l, speciesToClosestRelative) => (l.slice(0, 1), speciesToClosestRelative)
    }
      .groupByKey()
      .map {
        case (l, iter) => (l, iter.map(p => {
          val max = iter.foldRight(("", "", 0))((p1, p2) => if (p._1 != p1._2._1 && p1._2._3 > p2._3)
            (p1._2._1, s"kingdom - ${l(0)}", p1._2._3)
          else p2)
          if (p._2 == ("","",0)) (p._1, max) else p
        }))
      }
      .flatMapValues(p => p)

    val result = kingdomToSpeciesToClosestRelativeKingdomLvl.map {
      case (_, (s, (closestRelative, lvl, count))) => s"${s}\t${closestRelative}\t${lvl}\t${count}"
    }

    result.saveAsTextFile(args(1))
  }
}

