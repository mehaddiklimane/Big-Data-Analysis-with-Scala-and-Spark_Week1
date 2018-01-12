package wikipedia

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object Example {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WikipediaRanking").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val langs = List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    //val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath)
                                           .map(line => WikipediaData.parse(line))

    def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
      rdd.filter(f => f.mentionsLanguage(lang)).map(f => (lang,1)).aggregate(0)((k,v) => {
        k + v._2
      }, (u,x) => u+x)
    }

    //wikiRdd.foreach(r => println("title : "+r.title,"text : "+r.text))
    //lines.map(s => println(s)).take(3)
    // line.foreach(l => println(l))
    //def sumOfPlusOnes = sc.parallelize(List(1, 2, 3, 4, 5)).map(_ + 1).reduce(_ + _)

    //def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.filter(_.mentionsLanguage(lang)).count().toInt

    //val res = wikiRdd.filter(_.mentionsLanguage("Jav"))
    //println(res.count())
    //println(occurrencesOfLang(langs(0),wikiRdd))

    def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
      return langs.map(lang => (lang,occurrencesOfLang(lang,rdd))).sortBy(_._2).reverse
    }

    //println(rankLangs(langs,wikiRdd))

    def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
      sc.parallelize(langs.map(lang =>  {
        (lang,rdd.filter(_.mentionsLanguage(lang)).collect().toIterable)
      }))
    }

    def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
      return index.map(i => (i._1,i._2.size)).collect().toList.sortBy(_._2).reverse
    }

    //val index = makeIndex(langs,wikiRdd)
    //val res = rankLangsUsingIndex(index)
    //res.foreach(r => println(r))

    //val res1 = rankLangs(langs,wikiRdd)
    //res1.foreach(r => println(r))

    //.map(r => { r._2.map( l => (r._1,1))})


  }
}

