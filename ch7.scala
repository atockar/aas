ssh -i SparkAA.pem hadoop@ec2-52-62-37-223.ap-southeast-2.compute.amazonaws.com
mkdir medline_data
cd medline_data
wget ftp://ftp.nlm.nih.gov/nlmdata/sample/medline/*.gz
gunzip *.gz
ls -ltr
hadoop fs -mkdir -p medline
hadoop fs -put *.xml medline

cd ..
wget https://github.com/sryza/aas/archive/1st-edition.zip
unzip 1st-edition.zip
cd aas-1st-edition/common/
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn package
spark-shell --jars target/common-1.0.2.jar

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
val elem = XML.loadString(raw_xml)

  // Explore xml element
elem.label
elem.attributes
elem \ "MeshHeadingList"
elem \\ "MeshHeading"
(elem \\ "DescriptorName").map(_.text)
def majorTopics(elem: Elem): Seq[String] = {
 val dn = elem \\ "DescriptorName"
 val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
 mt.map(n => n.text)
}
majorTopics(elem)

// Get major topics of whole corpus
import org.apache.spark.rdd._   // added
val mxml: RDD[Elem] = medline_raw.map(XML.loadString)
val medline: RDD[Seq[String]] = mxml.map(majorTopics).cache()
medline.take(1)(0)

// ASIDE - finding number distinct authors
def authors(elem: Elem): Seq[(String)] = {
 val auth = elem \\ "Author"
 val valid = auth.filter(n => (n \ "@ValidYN").text == "Y")
 val ln = valid \ "LastName"
 val fn = valid \ "ForeName"
 val nameTup = ln.map(_.text).zip(fn.map(_.text))
 nameTup.map(x=>(x._2 + " " + x._1).toLowerCase())
}
val auths: RDD[Seq[String]] = mxml.map(authors)
val fAuths = auths.flatMap(x=>x).cache()
val allAuths = fAuths.distinct
allAuths.count

// ASIDE - finding count by author
val authCounts = fAuths.countByValue().toSeq
val sAuthCounts = authCounts.sortBy(_._2).reverse

// Analyzing co-occurrence
medline.count()
val topics: RDD[String] = medline.flatMap(mesh => mesh)
val topicCounts = topics.countByValue()
topicCounts.size
val tcSeq = topicCounts.toSeq
tcSeq.sortBy(_._2).reverse.take(10).foreach(println)

  // Distribution of topic counts
val valueDist = topicCounts.groupBy(_._2).mapValues(_.size)
valueDist.toSeq.sorted.take(10).foreach(println)

  // Create pairs
val list = List(1, 2, 3)
val combs = list.combinations(2)
combs.foreach(println)

val combs = list.reverse.combinations(2)
combs.foreach(println)
List(3, 2) == List(2, 3)

  // However need to order lists first, as shown above
val topicPairs = medline.flatMap(t => t.sorted.combinations(2))
val cooccurs = topicPairs.map(p => (p, 1)).reduceByKey(_+_)
cooccurs.cache()
cooccurs.count()

  // Find most frequent pairs
val ord = Ordering.by[(Seq[String], Int), Int](_._2)
cooccurs.top(10)(ord).foreach(println)

// ASIDE - create author pairs
val authPairs = auths.flatMap(_.sorted.combinations(2))

// Analyse cooccurence in GraphX
  // Apply numerical ids to topic (vertex) names
import com.google.common.hash.Hashing
def hashId(str: String) = {
 Hashing.md5().hashString(str).asLong()
}
val vertices = topics.map(topic => (hashId(topic), topic))
val uniqueHashes = vertices.map(_._1).countByValue()
val uniqueTopics = vertices.map(_._2).countByValue()
uniqueHashes.size == uniqueTopics.size

  // Create edges from cooccurs RDD
import org.apache.spark.graphx._
val edges = cooccurs.map(p => {
 val (topics, cnt) = p
 val ids = topics.map(hashId).sorted
 Edge(ids(0), ids(1), cnt)
})

  // Create graph
val topicGraph = Graph(vertices, edges)
topicGraph.cache()
vertices.count()
topicGraph.vertices.count()   // deduplicates vertices, not edges

// Investigate connectedness
val connectedComponentGraph: Graph[VertexId, Int] = topicGraph.connectedComponents()

  // List all connected components, sorted by size
def sortedConnectedComponents(connectedComponents: Graph[VertexId, _]): Seq[(VertexId, Long)] = {
 val componentCounts = connectedComponents.vertices.map(_._2).countByValue
 componentCounts.toSeq.sortBy(_._2).reverse
}

val componentCounts = sortedConnectedComponents(connectedComponentGraph)
componentCounts.size
componentCounts.take(10).foreach(println)

  // Find topics of one of the smaller graphs
val nameCID = topicGraph.vertices.innerJoin(connectedComponentGraph.vertices) {
 (topicId, name, componentId) => (name, componentId)
}
val c1 = nameCID.filter(x => x._2._2 == componentCounts(1)._1)
c1.collect().foreach(x => println(x._2._1))

  // Investigate HIV topics to discover why not connected
val hiv = topics.filter(_.contains("HIV")).countByValue()
hiv.foreach(println)

// Degree distribution
val degrees: VertexRDD[Int] = topicGraph.degrees.cache()
degrees.map(_._2).stats()

  // Check results
val sing = medline.filter(x => x.size == 1)
sing.count()
val singTopic = sing.flatMap(topic => topic).distinct()
singTopic.count()
val topic2 = topicPairs.flatMap(p => p)
singTopic.subtract(topic2).count()

  // Investigate top degrees
def topNamesAndDegrees(degrees: VertexRDD[Int],
 topicGraph: Graph[String, Int]): Array[(String, Int)] = {
 val namesAndDegrees = degrees.innerJoin(topicGraph.vertices) {
  (topicId, degree, name) => (name, degree)
 }
 val ord = Ordering.by[(String, Int), Int](_._2)
 namesAndDegrees.map(_._2).top(10)(ord)
}
topNamesAndDegrees(degrees, topicGraph).foreach(println)

// Interestingness of edges using EdgeTriplets
val T = medline.count()
val topicCountsRdd = topics.map(x => (hashId(x), 1)).reduceByKey(_+_)
val topicCountGraph = Graph(topicCountsRdd, topicGraph.edges)
def chiSq(YY: Int, YB: Int, YA: Int, T: Long): Double = {
 val NB=T-YB
 val NA=T-YA
 val YN=YA-YY
 val NY=YB-YY
 val NN=T-NY-YN-YY
 val inner = math.abs(YY*NN-YN*NY) - T / 2.0
 T*math.pow(inner,2)/(YA*NA*YB*NB)
}
val chiSquaredGraph = topicCountGraph.mapTriplets(triplet => {
 chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, T)
})
chiSquaredGraph.edges.map(x => x.attr).stats()

  // Filter using chisquared test statistic
val interesting = chiSquaredGraph.subgraph(triplet => triplet.attr > 19.5)
interesting.edges.count

  // Analyse filtered graph
val interestingComponentCounts = sortedConnectedComponents(interesting.connectedComponents())
interestingComponentCounts.size
interestingComponentCounts.take(10).foreach(println)

val interestingDegrees = interesting.degrees.cache()
interestingDegrees.map(_._2).stats()
topNamesAndDegrees(interestingDegrees, topicGraph).foreach(println)

// Calculate clustering coefficients
val triCountGraph = topicGraph.triangleCount()
triCountGraph.vertices.map(x => x._2).stats()

val maxTrisGraph = topicGraph.degrees.mapValues(d => d * (d - 1) / 2.0)

val clusterCoefGraph = triCountGraph.vertices.innerJoin(maxTrisGraph) {
 (vertexId, triCount, maxTris) => {
  if (maxTris == 0) 0 else triCount / maxTris
 }
}
clusterCoefGraph.map(_._2).sum() / topicGraph.vertices.count()   // average

// Average path length
def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
 def minThatExists(k: VertexId): Int = {
  math.min(
   m1.getOrElse(k, Int.MaxValue),
   m2.getOrElse(k, Int.MaxValue)
  )
 }
 (m1.keySet ++ m2.keySet).map {
  k => (k, minThatExists(k))
 }.toMap
}

def update(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int]) = {
 mergeMaps(state, msg)
}

def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId) = {
 val aplus=a.map{
  case(v,d)=>v->(d+1)
 }
 if (b != mergeMaps(aplus, b)) {
  Iterator((bid, aplus))
 } else {
  Iterator.empty
 }
}

def iterate(e: EdgeTriplet[Map[VertexId, Int], _]) = {
 checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
 checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
}

val fraction = 0.02
val replacement = false
val sample = interesting.vertices.map(v => v._1).sample(replacement, fraction, 1729L)
val ids = sample.collect().toSet

val mapGraph = interesting.mapVertices((id, _) => {
 if (ids.contains(id)) {
  Map(id -> 0)
 } else {
  Map[VertexId, Int]()
 }
})

val start = Map[VertexId, Int]()
val res = mapGraph.pregel(start)(update, iterate, mergeMaps)

val paths = res.vertices.flatMap {
 case (id, m) => m.map {
  case (k, v) =>
   if(id<k) {
    (id, k, v)
   } else {
    (k, id, v)
   }
 }
}.distinct()
paths.cache()

paths.map(_._3).filter(_ > 0).stats()

val hist = paths.map(_._3).countByValue()
hist.toSeq.sorted.foreach(println)