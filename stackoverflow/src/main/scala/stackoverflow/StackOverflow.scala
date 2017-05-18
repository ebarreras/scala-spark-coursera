package stackoverflow

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
//    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    def isValid(p: Posting): Boolean =
      p.postingType == 1 || (p.postingType == 2 && p.parentId.isDefined)

    val partitioned = postings.filter(isValid(_)).map(p => if (p.postingType == 1) (p.id, p) else (p.parentId.get, p))
    val questions = partitioned.filter(_._2.postingType == 1)
    val answers = partitioned.filter { case (_, p) => p.postingType == 2 && p.parentId.isDefined }
    questions.join(answers).groupByKey()
  }

  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

//    def answerHighScore(as: Array[Posting]): Int = {
//      var highScore = 0
//      var i = 0
//      while (i < as.length) {
//        val score = as(i).score
//            if (score > highScore)
//              highScore = score
//              i += 1
//      }
//      highScore
//    }

    grouped.mapValues(qAndAs => {
      val it = qAndAs.iterator
      var maxScore = Int.MinValue
      var item: (Posting,Posting) = null
      while (it.hasNext) {
        item = it.next
        if (item._2.score > maxScore) maxScore = item._2.score
      }
      (item._1, maxScore)
    }).values
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: String, ls: List[String]): Option[Int] = {
      @annotation.tailrec
      def go(pos: Int, ls: List[String]): Option[Int] = {
        if (ls.isEmpty) None
        else if (tag == ls.head) Some(pos)
        else go(pos + 1, ls.tail)
      }
      go(0, ls)
    }

    scored
      .map { case (question, score) => {
        val lang = question.tags.flatMap(firstLangInTag(_, langs)).getOrElse(-1)
        if (lang == -1) (-1, score) else (lang*langSpread, score)
      }}
      .filter { case (l, _) => l != -1 }
      .persist()
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

//    val addVector = (acc: ((Int, Int), Int), v: (Int, Int)) => ((acc._1._1 + v._1, acc._1._2 + v._2), acc._2 + 1)
//    val combine = (acc1: ((Int, Int), Int), acc2: ((Int, Int), Int)) => ((acc1._1._1 + acc2._1._1, acc1._1._2 + acc2._1._2), acc1._2 + acc2._2)
//    val partitioned = vectors.map(v => (findClosest(v, means), v)).persist;
//    val collected = partitioned.aggregateByKey(((0, 0), 0))(addVector, combine).collectAsMap()
//    val newCenters = collected.mapValues {
//      case ((x, y), n) => (x/n, y/n)
//    }
//    val newMeans = (0 until means.length).map { i => newCenters.get(i).getOrElse(means(i))}.toArray

    //    val kMeansPartitioner = new Partitioner {
//      override def numPartitions: Int = means.length
//      override def getPartition(key: Any): Int = key.hashCode()
//    }
//
    val newCentroids = vectors.map(v => (findClosest(v, means), v)).groupByKey().mapValues(vs => averageVectors(vs)).collectAsMap()
    val newMeans = (0 until means.length).map { i => newCentroids.get(i).getOrElse(means(i))}.toArray

    //if (debug) {
      println(s"Old means length: ${means.length}, new means length: ${newMeans.length}")
    //}

//    val newMeans = means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }

  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey().persist()

    val median = closestGrouped.mapValues { vs =>
      // most common language in the cluster
      val langIndex = vs.toStream.map(_._1).groupBy(x => x).mapValues(_.size).toSeq.sortBy(-_._2).map(_._1).head
      val langLabel: String   = langs(langIndex/langSpread)

      // percent of the questions in the most common language
      val clusterSize: Int    = vs.size
      val langPercent: Double = vs.filter(_._1 == langIndex).size * 100 / clusterSize
      val sortedScores = vs.map(_._2).toArray.sorted
      val medianScore: Int = {
        if (sortedScores.size % 2 == 0)
          (sortedScores(vs.size/2) + sortedScores(vs.size/2 - 1)) / 2
        else
          sortedScores(vs.size/2)
      }
      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
