package search.sol

import java.io._

import search.src.{FileIO, PorterStemmer, StopWords}

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Represents a query REPL built off of a specified index
 *
 * @param titleIndex    - the filename of the title index
 * @param documentIndex - the filename of the document index
 * @param wordIndex     - the filename of the word index
 * @param usePageRank   - true if page rank is to be incorporated into scoring
 */
class Query(titleIndex: String, documentIndex: String, wordIndex: String,
            usePageRank: Boolean) {
  
  // Maps the document ids to the title for each document
  private val idsToTitle = new mutable.HashMap[Int, String]
  
  private val idsToMaxFreqs = new mutable.HashMap[Int, Double]
  
  // Maps the document ids to the page rank for each document
  private val idsToPageRank = new mutable.HashMap[Int, Double]
  
  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word
  private val wordsToDocumentFrequencies = new mutable.HashMap[String, mutable.HashMap[Int, Double]]
  
  /**
   * Handles a single query and prints out results
   *
   * @param userQuery - the query text
   */
  private def query(userQuery: String) {
    val t1 = System.nanoTime()
    
    val idsToScores = new mutable.HashMap[Int, Double]
    val regex = new Regex("""[\w]+""")
    val matchesIterator = regex.findAllMatchIn(userQuery)
    val matchesList = matchesIterator.toList.map { x => x.matched }
    
    def termIterator(mList: List[String]): List[String] = mList match {
      case Nil => Nil
      case hd :: tl if StopWords.isStopWord(hd.toLowerCase()) => termIterator(tl)
      case hd :: tl => PorterStemmer.stem(hd.toLowerCase()) :: termIterator(tl)
    }
    
    val termList = termIterator(matchesList)
    
    for (term <- termList) {
      if (wordsToDocumentFrequencies.get(term).isDefined) {
        val idFreqMap = wordsToDocumentFrequencies(term)
        val idf = Math.log(idsToMaxFreqs.size.toDouble / idFreqMap.size.toDouble)
        
        for (freqPair <- idFreqMap) {
          val currID = freqPair._1
          if (usePageRank) {
            idsToScores.get(currID) match {
              case None =>
                idsToScores.put(currID,
                  idf * freqPair._2 / idsToMaxFreqs(currID) * idsToPageRank(currID))
              case Some(score) =>
                idsToScores.update(currID,
                  score + idf * freqPair._2 / idsToMaxFreqs(currID) * idsToPageRank(currID))
            }
          }
          else {
            idsToScores.get(currID) match {
              case None =>
                idsToScores.put(currID,
                  idf * freqPair._2 / idsToMaxFreqs(currID))
              case Some(score) =>
                idsToScores.update(currID,
                  score + idf * freqPair._2 / idsToMaxFreqs(currID))
            }
          }
        }
      }
    }
    
    for ((x, y) <- idsToScores) {
      if (y == 0) idsToScores.remove(x)
    }
    if (idsToScores.isEmpty) {
      println("No documents found")
    } else {
      val p = if (idsToScores.size > 10) 10 else idsToScores.size
      val topIDs = new Array[Int](p)
      val tempMap = idsToScores
      for (j <- 0 until p) {
        val firstID = maxScoredID(tempMap)
        topIDs(j) = firstID
        tempMap.remove(firstID)
      }
      printResults(topIDs)
      println("Time taken: " + (System.nanoTime() - t1) / 1e9d + "s")
    }
    
  }
  
  /**
   * maxScoredID returns the ID of the page with the highest score
   *
   * @param scoreMap a HashMap which maps the IDs to the scores
   * @return The ID with the maximum score in the map
   */
  private def maxScoredID(scoreMap: mutable.HashMap[Int, Double]): Int = {
    var score: Double = -1000000.0
    var id: Int = 0
    for ((x, y) <- scoreMap) {
      if (score < y) {
        score = y
        id = x
      }
    }
    id
  }
  
  /**
   * Format and print up to 10 results from the results list
   *
   * @param results - an array of all results
   */
  private def printResults(results: Array[Int]) {
    for (i <- 0 until Math.min(10, results.length)) {
      println("\t" + (i + 1) + " " + idsToTitle(results(i)))
    }
  }
  
  def readFiles(): Unit = {
    FileIO.readTitles(titleIndex, idsToTitle)
    FileIO.readDocuments(documentIndex, idsToMaxFreqs, idsToPageRank)
    FileIO.readWords(wordIndex, wordsToDocumentFrequencies)
  }
  
  /**
   * Starts the read and print loop for queries
   */
  def run() {
    val inputReader = new BufferedReader(new InputStreamReader(System.in))
    
    // Print the first query prompt and read the first line of input
    print("search> ")
    var userQuery = inputReader.readLine()
    
    // Loop until there are no more input lines (EOF is reached)
    while (userQuery != null) {
      // If ":quit" is reached, exit the loop
      if (userQuery == ":quit") {
        inputReader.close()
        return
      }
      
      // Handle the query for the single line of input
      query(userQuery)
      
      // Print next query prompt and read next line of input
      print("search> ")
      userQuery = inputReader.readLine()
    }
    
    inputReader.close()
  }
}

/**
 * An object of the class Query. Attempts to run a query.
 */
object Query {
  def main(args: Array[String]) {
    try {
      // Run queries with page rank
      var pageRank = false
      var titleIndex = 0
      var docIndex = 1
      var wordIndex = 2
      if (args.length == 4 && args(0) == "--pagerank") {
        pageRank = true
        titleIndex = 1
        docIndex = 2
        wordIndex = 3
      } else if (args.length != 3) {
        println("Incorrect arguments. Please use [--pagerank] <titleIndex> "
          + "<documentIndex> <wordIndex>")
        System.exit(1)
      }
      val query: Query = new Query(args(titleIndex), args(docIndex), args(wordIndex), pageRank)
      query.readFiles()
      query.run()
    } catch {
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
    }
  }
}