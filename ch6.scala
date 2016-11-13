cd Documents/Knowledge/Training/Cloudera/Advanced\ Analytics\ with\ Spark/
ssh -i SparkAA.pem hadoop@ec2-52-65-188-37.ap-southeast-2.compute.amazonaws.com
mkdir wiki
cd wiki
curl -s -L -o wikidump.xml.bz2 https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles10.xml-p002336425p003046511.bz2
// http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2 too big
du -sh *
hdfs dfs -mkdir -p wiki
bzip2 -cd wikidump.xml.bz2 | hdfs dfs -put - wiki/wikidump.xml
cd ..
wget https://github.com/sryza/aas/archive/1st-edition.zip
unzip 1st-edition.zip
cd aas-1st-edition
mvn package
hdfs dfs -put ~/aas-1st-edition/ch06-lsa/src/main/resources/stopwords.txt wiki/stopwords.txt
spark-shell --jars target/ch06-lsa-1.0.2-jar-with-dependencies.jar

// Install core nlp with sbt on aws
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum install sbt
wget https://github.com/gangeli/CoreNLP-Scala/archive/master.zip
unzip master.zip
cd CoreNLP-Scala-master/
sbt
 compile
 // run
 package


// Import xml file
import com.cloudera.datascience.common.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
val path = "wiki/wikidump.xml"
@transient val conf = new Configuration()
conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
val kvs = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],
 classOf[LongWritable], classOf[Text], conf)
val rawXmls = kvs.map(p => p._2.toString)

// Get article plain text
import edu.umd.cloud9.collection.wikipedia.language._
import edu.umd.cloud9.collection.wikipedia._
def wikiXmlToPlainText(xml: String): Option[(String, String)] = { 
 val page = new EnglishWikipediaPage()
 WikipediaPage.readPage(page, xml)
 if (page.isEmpty) None
 else Some((page.getTitle, page.getContent))
}
val plainText = rawXmls.flatMap(wikiXmlToPlainText)

// Lemmatization and stopwords
import scala.collection.JavaConversions._   // added in errata
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import java.util.Properties    // added as Properties not found
import scala.collection.mutable.ArrayBuffer    // added as not found
def createNLPPipeline(): StanfordCoreNLP = {
 val props = new Properties()
 props.put("annotators", "tokenize, ssplit, pos, lemma")
 new StanfordCoreNLP(props)
}
def isOnlyLetters(str: String): Boolean = {
 str.forall(c => Character.isLetter(c))
}
def plainTextToLemmas(text: String, stopWords: Set[String],
 pipeline: StanfordCoreNLP): Seq[String] = {
 val doc = new Annotation(text)
 pipeline.annotate(doc)
 val lemmas = new ArrayBuffer[String]()
 val sentences = doc.get(classOf[SentencesAnnotation])
 for (sentence <- sentences;
  token <- sentence.get(classOf[TokensAnnotation])) {
   val lemma = token.get(classOf[LemmaAnnotation])
   if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
    lemmas += lemma.toLowerCase
   }
 }
 lemmas
}

// val stopWords = sc.broadcast(scala.io.Source.fromFile("wiki/stopwords.txt").getLines().toSet).value
val stop = sc.textFile("wiki/stopwords.txt").collect().toSet
val stopWords = sc.broadcast(stop).value

import org.apache.spark.rdd._   // added
val lemmatized: RDD[Seq[String]] = plainText.mapPartitions(it => {
 val pipeline = createNLPPipeline()
 it.map { case(title, contents) =>
  plainTextToLemmas(contents, stopWords, pipeline)
 }
})

// Compute tf-idfs
def termDocWeight(termFrequencyInDoc: Int, totalTermsInDoc: Int,
 termFreqInCorpus: Int, totalDocs: Int): Double = {
  val tf = termFrequencyInDoc.toDouble / totalTermsInDoc
  val docFreq = totalDocs.toDouble / termFreqInCorpus
  val idf = math.log(docFreq)
  tf*idf
}  // Simple function, not used

import scala.collection.mutable.HashMap
val docTermFreqs = lemmatized.map(terms => {
 val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
  (map, term) => {
   map += term -> (map.getOrElse(term, 0) + 1)
   map
  }
 }
 termFreqs
})
docTermFreqs.cache()

val zero = new HashMap[String, Int]()
def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int]): HashMap[String, Int] = {
 tfs.keySet.foreach { term =>
  dfs += term -> (dfs.getOrElse(term, 0) + 1)
 }
 dfs
}
def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int]): HashMap[String, Int] = {
 for ((term, count) <- dfs2) {
  dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
 }
 dfs1
}
docTermFreqs.aggregate(zero)(merge, comb)

docTermFreqs.flatMap(_.keySet).distinct().count()

// Limit size
val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)
val numTerms = 50000
val ordering = Ordering.by[(String, Int), Int](_._2)
val topDocFreqs = docFreqs.top(numTerms)(ordering)

val numDocs = docTermFreqs.count()
val idfs = docFreqs.map{
 case (term, count) => (term, math.log(numDocs.toDouble / count))
}.collectAsMap()

val termToId = idfs.keys.zipWithIndex.toMap
val bTermToId = sc.broadcast(termToId).value
val bIdfs = sc.broadcast(idfs).value

import scala.collection.JavaConversions._
import org.apache.spark.mllib.linalg.Vectors
val vecs = docTermFreqs.map(termFreqs => {
 val docTotalTerms = termFreqs.values.sum
 val termScores = termFreqs.filter {
  case (term, freq) => bTermToId.contains(term)
 }.map {
  case (term, freq) => (bTermToId(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
 }.toSeq
 Vectors.sparse(bTermToId.size, termScores)
})

// SVD
import org.apache.spark.mllib.linalg.distributed.RowMatrix
vecs.cache()
val mat = new RowMatrix(termDocMatrix)
val k=1000
val svd = mat.computeSVD(k, computeU=true)

// Find important concepts
import scala.collection.mutable.ArrayBuffer
val v = svd.V
val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
val arr = v.toArray
for (i <- 0 until numConcepts) {
 val offs = i * v.numRows
 val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
 val sorted = termWeights.sortBy(-_._1)
 topTerms += sorted.take(numTerms).map {
  case (score, id) => (termIds(id), score)
 }
}
topTerms

def topDocsInTopConcepts(
 svd: SingularValueDecomposition[RowMatrix, Matrix],
 numConcepts: Int, numDocs: Int,docIds: Map[Long, String]
 ): Seq[Seq[(String, Double)]] = {
  val u = svd.U
  val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
  for (i <- 0 until numConcepts) {
   val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
   topDocs += docWeights.top(numDocs).map {
    case (score, id) => (docIds(id), score)
   }
  }
 topDocs
}

val topConceptTerms = topTermsInTopConcepts(svd, 4, 10, termIds)
val topConceptDocs = topDocsInTopConcepts(svd, 4, 10, docIds)
for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
 println("Concept terms: " + terms.map(_._1).mkString(", "))
 println("Concept docs: " + docs.map(_._1).mkString(", "))
 println()
}

// Remove junk pages
def wikiXmlToPlainText(xml: String): Option[(String, String)] = {
 ...
 if (page.isEmpty || !page.isArticle || page.isRedirect ||
  page.getTitle.contains("(disambiguation)")) {
  None
 } else {
  Some((page.getTitle, page.getContent))
 }
}

// Term-term relevance
import breeze.linalg.{DenseVector => BDenseVector}
import breeze.linalg.{DenseMatrix => BDenseMatrix}
def topTermsForTerm(normalizedVS: BDenseMatrix[Double], termId: Int): Seq[(Double, Int)] = {
 val rowVec = new BDenseVector[Double]( row(normalizedVS, termId).toArray)
 val termScores = (normalizedVS * rowVec).toArray.zipWithIndex
 termScores.sortBy(-_._1).take(10)
}
val VS = multiplyByDiagonalMatrix(svd.V, svd.s)
val normalizedVS = rowsNormalized(VS)
def printRelevantTerms(term: String) {
 val id = idTerms(term)
 printIdWeights(topTermsForTerm(normalizedVS, id, termIds)
}
printRelevantTerms("algorithm")

// Document-document relevance
import org.apache.spark.mllib.linalg.Matrices
def topDocsForDoc(normalizedUS: RowMatrix, docId: Long): Seq[(Double, Long)] = {
 val docRowArr = row(normalizedUS, docId)
 val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
 val docScores = normalizedUS.multiply(docRowVec)
 val allDocWeights = docScores.rows.map(_.toArray(0)).
  zipWithUniqueId() allDocWeights.filter(!_._1.isNaN).top(10)
}
val US = multiplyByDiagonalMatrix(svd.U, svd.s)
val normalizedUS = rowsNormalized(US)
def printRelevantDocs(doc: String) {
 val id = idDocs(doc)
 printIdWeights(topDocsForDoc(normalizedUS, id, docIds)
}
printRelevantDocs("Romania")
printRelevantDocs("Brad Pitt")
printRelevantDocs("Radiohead")

// Term-document relevance
def topDocsForTerm(US: RowMatrix, V: Matrix, termId: Int) : Seq[(Double, Long)] = {
 val rowArr = row(V, termId).toArray
 val rowVec = Matrices.dense(termRowArr.length, 1, termRowArr)
 val docScores = US.multiply(termRowVec)
 val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId()
 allDocWeights.top(10)
}
def printRelevantDocs(term: String) {
 val id = idTerms(term)
 printIdWeights(topDocsForTerm(normalizedUS, svd.V, id, docIds)
}
printRelevantDocs("fir")
printRelevantDocs("graph")

// Multiple term queries
import breeze.linalg.{SparseVector => BSparseVector}
def termsToQueryVector(terms: Seq[String],idTerms: Map[String, Int],
 idfs: Map[String, Double]): BSparseVector[Double] = {
  val indices = terms.map(idTerms(_)).toArray
  val values = terms.map(idfs(_)).toArray
  new BSparseVector[Double](indices, values, idTerms.size)
}
def topDocsForTermQuery(US: RowMatrix, V: Matrix,
 query: BSparseVector[Double]): Seq[(Double, Long)] = {
  val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
  val termRowArr = (breezeV.t * query).toArray
  val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)
  val docScores = US.multiply(termRowVec)
  val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId()
  allDocWeights.top(10)
}
def printRelevantDocs(terms: Seq[String]) {
 val queryVec = termsToQueryVector(terms, idTerms, idfs)
 printIdWeights(topDocsForTermQuery(US, svd.V, queryVec), docIds)
}
printRelevantDocs(Seq("factorization", "decomposition"))
