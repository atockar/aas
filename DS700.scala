//// Set up

// Read in xml files
import com.cloudera.datascience.common.XmlInputFormat
import org.apache.spark.SparkContext
import org.apache.hadoop.io.{Text, LongWritable} 
import org.apache.hadoop.conf.Configuration
def loadMedline(sc: SparkContext, path: String) = {
 @transient val conf = new Configuration()
 conf.set(XmlInputFormat.START_TAG_KEY, "<MedlineCitation ")
 conf.set(XmlInputFormat.END_TAG_KEY, "</MedlineCitation>")
 val in = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],
  classOf[LongWritable], classOf[Text], conf)
 in.map(line => line._2.toString)
}
val medline_raw = loadMedline(sc, "medline")

// Parse xml
import scala.xml._
val raw_xml = medline_raw.take(1)(0)
import org.apache.spark.rdd._   // added
val mxml: RDD[Elem] = medline_raw.map(XML.loadString)

//// Q1 ** need to do something about encoding possibly **

// Map out authors (case sensitive)
def authorsCS(elem: Elem): Seq[(String)] = {
 val auth = elem \\ "Author"
 // val valid = auth.filter(n => (n \ "@ValidYN").text == "Y")
 val ln = auth \ "LastName"
 val fn = auth \ "ForeName"
 val nameTup = ln.map(_.text).zip(fn.map(_.text))
 nameTup.map(x=>(x._2 + " " + x._1))
}
val authsCS: RDD[Seq[String]] = mxml.map(authorsCS)
val fAuthsCS = authsCS.flatMap(x=>x).cache()

// Map out authors (case insensitive)
def authorsCI(elem: Elem): Seq[(String)] = {
 val auth = elem \\ "Author"
 // val valid = auth.filter(n => (n \ "@ValidYN").text == "Y")
 val ln = auth \ "LastName"
 val fn = auth \ "ForeName"
 val nameTup = ln.map(_.text).zip(fn.map(_.text))
 nameTup.map(x=>(x._2 + " " + x._1).toUpperCase())
}
val authsCI: RDD[Seq[String]] = mxml.map(authorsCI)
val fAuthsCI = authsCI.flatMap(x=>x).cache()

// Count distinct
val q1a = fAuthsCS.distinct.count // 481833
val q1b = fAuthsCI.distinct.count // 473172

// List those with different cases
// Iterate over all distinct case sensitive authors, cast to upper, then store in count array
val upperCS = fAuthsCS.distinct.map(_.toUpperCase())
val q1c = upperCS.countByValue().filter(_._2>1).map(_._1)
q1c.size  // 8637 lines
sc.parallelize(q1c.toSeq).saveAsTextFile("q1c")

// Output for Python check
fAuthsCS.distinct.saveAsTextFile("check/authsCS")

//// Q2

// Create author pairs
val authPairs = authsCI.flatMap(_.sorted.combinations(2))

// Count and distribution
val cooccurs = authPairs.map(p => (p, 1)).reduceByKey(_+_)
val coValues = cooccurs.map(x => (x._2,1)).reduceByKey(_+_)
val coSorted = coValues.sortByKey(true).map(x => x._1 + "\t" + x._2)
coSorted.saveAsTextFile("q2a")

// Analyze using GraphX

  // Apply numerical ids to topic (vertex) names
import com.google.common.hash.Hashing
def hashId(str: String) = {
 Hashing.md5().hashString(str).asLong()
}
val vertices = fAuthsCI.map(auth => (hashId(auth), auth))
  // Check no duplicate hashes
val uniqueHashes = vertices.map(_._1).countByValue()
val uniqueAuths = vertices.map(_._2).countByValue()
uniqueHashes.size == uniqueAuths.size

  // Create edges from cooccurs RDD
import org.apache.spark.graphx._
val edges = cooccurs.map(p => {
 val (auths, cnt) = p
 val ids = auths.map(hashId).sorted
 Edge(ids(0), ids(1),1)
})

  // Create graph
val authGraph = Graph(vertices, edges)
authGraph.cache()
vertices.count()
authGraph.vertices.count()   // deduplicates vertices, not edges

// Degree distribution - Q2b & Q2c. But perhaps want to add zeros?
val degrees: VertexRDD[Int] = authGraph.degrees.cache()
degrees.map(_._2).stats()  // (count: 427179, mean: 5.222078, stdev: 5.885429, max: 270, min: 1)
val degs = degrees.map(_._2).collect().sorted
val median = (degs(degs.length/2) + degs(degs.length - degs.length/2)) / 2   // 4

  // Check difference in count
val sing = authsCI.filter(x => x.size == 1)
sing.count()  // 79660
val singAuth = sing.flatMap(x=>x).distinct()
singAuth.count()  // 64384
val auth2 = authPairs.flatMap(x=>x)
singAuth.subtract(auth2).count()  // 45993 (correct)

  // Check in python and perhaps add 0s
degrees.map(_._2).saveAsTextFile("check/degrees")

// Q2d was incorrect in exam, double check wording and ensure distribution fit makes sense

//// Q3

// Add language to cooccurence tuples
def lang_aCI(elem: Elem): Seq[(String)] = {
 val lang = elem\\ "Language"
 val auth = elem \\ "Author"
 val ln = auth \ "LastName"
 val fn = auth \ "ForeName"
 val nameTup = ln.map(_.text).zip(fn.map(_.text))
 val authList = nameTup.map(x=>(x._2 + " " + x._1).toUpperCase())
 lang.map(_.text) ++ authList
}
val langAuth: RDD[Seq[String]] = mxml.map(lang_aCI)

// Create an RDD filtered by each eligible language then follow the steps for triangle counting
val langs = langAuth.map(x=>(x.head,1)).reduceByKey(_+_).filter(_._2>=1000)
langs.collect()  // 11 languages

val langList = langs.map(_._1).collect().toSeq

var out = List("####")

for (l <- langList) {
  var auths = langAuth.filter(x=>x.head==l).map(_.tail)
  var aPairs = auths.flatMap(_.sorted.combinations(2))
  var aCooc = aPairs.map(p => (p, 1)).reduceByKey(_+_)

  var verts = auths.flatMap(x=>x).map(a=>(hashId(a),a))
  var edges = aCooc.map(p => {
   val (auths, cnt) = p
   val ids = auths.map(hashId).sorted
   Edge(ids(0), ids(1),1)
  })
  var aGraph = Graph(verts, edges)
  aGraph.cache()

  // Calculate clustering coefficients
  var triCountGraph = aGraph.triangleCount()
  // triCountGraph.vertices.map(x => x._2).stats()

  var maxTrisGraph = aGraph.degrees.mapValues(d => d * (d - 1) / 2.0) // number of possible edges

  var clusterCoefGraph = triCountGraph.vertices.innerJoin(maxTrisGraph) {
   (vertexId, triCount, maxTris) => {
    if (maxTris == 0) 0 else triCount / maxTris
   }
  }
  var result = l + ": " + (clusterCoefGraph.map(_._2).sum() / aGraph.vertices.count())   // average
  println(result)
  out = out ++ List(result)

  aGraph.unpersist()
}
out.foreach(println)