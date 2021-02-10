package search.sol

import java.io.{FileNotFoundException, IOException}

import search.src.{FileIO, PorterStemmer, StopWords}

import scala.collection.mutable
import scala.util.matching.Regex
import scala.xml.{Node, NodeSeq}

/**
 * This is an indexer class which preprocesses the corpus/ an xml file and
 * applies pageRank to each of the page in the xml file. The indexer finally
 * saves the titles, the document frequencies/pageRank values and the word
 * frequencies in three text files
 *
 * @param corpus      the main xml file containing the pages
 * @param titleTxt    the text file which maps document IDs to document titles
 * @param documentTxt the text file which stores the rankings computed by
 *                    PageRank
 * @param wordTxt     the text file which stores the relevance of documents to words
 */

class Index(corpus: String, titleTxt: String, documentTxt: String, wordTxt: String) {
  private val mainNode: Node = xml.XML.loadFile(corpus)
  private var pageSeq = (mainNode \ "page")
  private val pageSeqLength= pageSeq.length
  private val idSeq: NodeSeq = pageSeq \ "id"
  
  //Converts pageSeq into an array, a mutable data structure which can be
  // cleared as needed.
  private val pageArr = pageSeq.toArray
  
  //Clears out pageSeq for garbage collection.
  pageSeq = null
  
  private val regex = new Regex("""[^\W_]+'[^\W_]+|[^\W_]+""")
  private val regex2 = new Regex("""\[\[[^\[]+?\]\]""")
  
  private val idsToTitles: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  private val linksHash: mutable.HashMap[Int, mutable.Set[String]] =
    new mutable.HashMap[Int, mutable.Set[String]]()
  private val wordsToDocumentFrequencies: mutable.HashMap[String, mutable.HashMap[Int, Double]] =
    new mutable.HashMap[String, mutable.HashMap[Int, Double]]
  private val maxFreqs: mutable.HashMap[Int, Double] = new mutable.HashMap[Int, Double]()
  private val idsToPageRank = new mutable.HashMap[Int, Double]()
  private val titlesArray = new Array[String](idSeq.length)
  
  /**
   * index is the method in which parsing is done and pageRank values are
   * computed using other helper functions.
   */
  private def index(): Unit = {
    var i = 0
    for (page <- pageArr) {
      titlesArray(i) = page.\("title").text.trim
      idsToTitles.put(page.\("id").text.trim.toInt, titlesArray(i))
      i += 1
    }
    
    var j = 0
    for (page <- pageArr) {
      val currID = page.\("id").text.trim.toInt
      var maxCount = 1.0
      maxCount = parse(page.\("title").text, currID, maxCount)
      maxCount = parse(page.\("text").text, currID, maxCount)
      maxFreqs.put(currID, maxCount)
      parseHelp(page.\("text").text, currID, maxCount)
      pageArr(j) = null
      j += 1
    }
    pageRank()
    
  }
  
  /**
   * parse is a method which stores all the words of the page with their
   * frequencies in a HashMap. It also helps to store the ids to links in
   * another HashMap using helper functions.
   *
   * @param words a string consisting of all the words of a page
   * @param id    an int indicating the id of the page being parsed
   * @param maxCount a double corresponding to the maximum number of occurrences
   *                 of a single word
   */
  private def parse(words: String, id: Int, maxCount: Double): Double = {
    val matchesIterator = regex.findAllMatchIn(words)
    val matchesList = matchesIterator.toList.map { x => x.matched }
    val matchMap = matchesList.map(x => x.toLowerCase())
    
    var newMaxCount = maxCount
    
    def tokenize(matchStr: String): Unit = {
      if (!StopWords.isStopWord(matchStr) &&
        (!matchStr.startsWith("[[") || !matchStr.endsWith("]]"))) {
        val stemmed = PorterStemmer.stem(matchStr)
        if (!wordsToDocumentFrequencies.contains(stemmed)) {
          val idToFreq: mutable.HashMap[Int, Double] = new mutable.HashMap[Int, Double]()
          idToFreq.put(id, 1)
          wordsToDocumentFrequencies.put(stemmed, idToFreq)
        }
        else {
          if (wordsToDocumentFrequencies(stemmed).contains(id)) {
            val newHash = wordsToDocumentFrequencies(stemmed)
            val newFreq = wordsToDocumentFrequencies(stemmed)(id) + 1
            if (newFreq > newMaxCount) {
              newMaxCount = newFreq
            }
            newHash.put(id, newFreq)
            wordsToDocumentFrequencies(stemmed).put(id, newFreq)
          }
          else {
            wordsToDocumentFrequencies(stemmed).update(id, 1)
          }
        }
      }
    }
    
    for (matchStr <- matchMap) {
      tokenize(matchStr)
    }
    newMaxCount
  }
  
  /**
   * Helper to parse, handles links
   *
   * @param words - the link
   * @param id - the id of the page the link is on
   * @param maxCount - the current maxFreq of the current page
   */
  private def parseHelp(words: String, id: Int, maxCount: Double): Unit ={
    val linkIterator = regex2.findAllMatchIn(words.trim)
    val linkArr = linkIterator.toList.map { x => x.matched }
  
    def parseLinks(linkList: List[String], id: Int): mutable.Set[String] = {
      val linkSet: mutable.Set[String] = mutable.Set()
      val linkTitle = linkList.map(a => a.replaceAll("""\|[\w+\s]+\]\]|\[\[|\]\]""",
        ""))
      val linkTitles = linkTitle.filter(x => !x.equals(idsToTitles(id)))
    
      for (link <- linkTitles) {
        val splitList = link.split("\\|")
        linkSet += splitList(0)
        if (splitList.length == 2) {
          val afterPipe = splitList(1)
          parse(afterPipe, id, maxCount)
        }
      }
      val finalSet = linkSet.filter(x => titlesArray.contains(x))
      if (finalSet.isEmpty) {
        for (i <- titlesArray) {
          if (!i.equals(idsToTitles(id))) {
            finalSet += i
          }
        }
      }
      finalSet
    }
  
    linksHash.put(id, parseLinks(linkArr, id))
  }
  
  /**
   * A method which computes rankings using the PageRank algorithm
   */
  private def pageRank(): Unit = {
    def dist(x: Array[Double], y: Array[Double]): Double = {
      var s = 0.0
      for (i <- x.indices) {
        s += Math.pow(x(i) - y(i), 2.0)
      }
      Math.sqrt(s)
    }
    
    val smallNum: Double = 0.15
    val r: Array[Double] = new Array[Double](pageSeqLength)
    val r1: Array[Double] = new Array[Double](pageSeqLength)
    val n = r.length
    val num = 1 / n.toDouble
    val idNodeArray = idSeq.toArray
    val idArray = new Array[Int](idNodeArray.length)
    for (i <- 0 until idSeq.length) {
      idArray(i) = idNodeArray(i).text.trim.toInt
    }
    
    for (i <- r.indices) {
      r(i) = 0.0
      r1(i) = num
    }
    while (dist(r1, r) > 0.001) {
      for (i <- r.indices) {
        r(i) = r1(i)
      }
      for (j <- 0 until n) {
        r1(j) = 0
        for (k <- 0 until n) {
          val w: Double = {
            if (linksHash(idArray(k)).contains(idsToTitles(idArray(j)))) {
              (smallNum / n) +
                ((1.0 - smallNum) * (1 / linksHash(idArray(k)).size.toDouble))
            } else {
              smallNum / n
            }
          }
          r1(j) = r1(j) + w * r(k)
        }
      }
    }
    for (i <- r.indices) {
      idsToPageRank.put(idArray(i), r(i))
      
    }
  }
}

/**
 * An object of the class Index to call the main function.
 */
object Index {
  /**
   * the main function of an object of class index
   *
   * @param args the arguments as an array of strings
   */
  def main(args: Array[String]) {
    val t1 = System.nanoTime()
    try {
      if (args.length == 4) {
        val index = new Index(args(0), args(1), args(2), args(3))
        index.index()
        FileIO.printTitleFile(args(1), index.idsToTitles)
        FileIO.printDocumentFile(args(2), index.maxFreqs, index.idsToPageRank)
        FileIO.printWordsFile(args(3), index.wordsToDocumentFrequencies)
        println("Time taken: " + (System.nanoTime() - t1) / 1e9d + "s")
      } else {
        println("Incorrect arguments. Please use <corpus file name> <title file name> " +
          "<document file name> <word file name>.")
        System.exit(1)
      }
    }
    catch{
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
      }
    }
  
}