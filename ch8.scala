ssh -i SparkAA.pem hadoop@
mkdir taxidata
cd taxidata
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_1.csv.zip
unzip trip_data_1.csv.zip
head -n 10 trip_data_1.csv

hadoop fs -mkdir -p taxidata
hadoop fs -put trip_data_1.csv taxidata

wget https://nycdatastables.s3.amazonaws.com/2013-08-19T18:15:35.172Z/nyc-borough-boundaries-polygon.geojson
mv nyc-borough-boundaries-polygon.geojson nyc-boroughs.geojson

cd ..
wget https://github.com/sryza/aas/archive/1st-edition.zip
unzip 1st-edition.zip
cd aas-1st-edition
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn package
cd ch08-geotime
spark-shell --jars target/ch08-geotime-1.0.0.jar

// // DateTime libraries
// import com.github.nscala_time.time.Imports._
// val dt1 = new DateTime(2014, 9, 4, 9, 0)
// dt1.dayOfYear.get
// val dt2 = new DateTime(2014, 10, 31, 15, 0)
// dt1 < dt2
// val dt3 = dt1 + 60.days
// dt3 > dt2

// import java.text.SimpleDateFormat
// val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
// val date = format.parse("2014-10-12 10:30:44")
// val datetime = new DateTime(date)

// val d = new Duration(dt1, dt2)
// d.getMillis
// d.getStandardHours
// d.getStandardDays

// // Enrich Esri Geometry API
// import com.esri.core.geometry.Geometry
// import com.esri.core.geometry.GeometryEngine
// import com.esri.core.geometry.SpatialReference
// class RichGeometry(val geometry: Geometry,
//  val spatialReference: SpatialReference = SpatialReference.create(4326)) {
//   def area2D() = geometry.calculateArea2D()
//   def contains(other: Geometry): Boolean = {
//    GeometryEngine.contains(geometry, other, spatialReference)
//   }
//   def distance(other: Geometry): Double = {
//    GeometryEngine.distance(geometry, other, spatialReference
//   }
// }
// object RichGeometry {
//  implicit def wrapRichGeo(g: Geometry) = {
//   new RichGeometry(g)
//  }
// }
// import RichGeometry._

// // Create GeoJSON parser with Spray
// import spray.json.JsValue
// case class Feature(val id: Option[JsValue],
//  val properties: Map[String, JsValue], val geometry: RichGeometry) {
//   def apply(property: String) = properties(property)
//   def get(property: String) = properties.get(property)
// }
// case class FeatureCollection(features: Array[Feature]) extends IndexedSeq[Feature] {
//  def apply(index: Int) = features(index)
//  def length = features.length
// }

// implicit object FeatureJsonFormat extends RootJsonFormat[Feature] {
//  def write(f: Feature) = {
//   val buf = scala.collection.mutable.ArrayBuffer(
//    "type" -> JsString("Feature"),
//    "properties" -> JsObject(f.properties),
//    "geometry" -> f.geometry.toJson)
//   f.id.foreach(v => { buf += "id" -> v})
//   JsObject(buf.toMap)
//  }
//  def read(value: JsValue) = {
//   val jso = value.asJsObject
//   val id = jso.fields.get("id")
//   val properties = jso.fields("properties").asJsObject.fields
//   val geometry = jso.fields("geometry").convertTo[RichGeometry]
//   Feature(id, properties, geometry)
//  }
// }

// Load taxi data
val taxiRaw = sc.textFile("taxidata")
val taxiHead = taxiRaw.take(10)
taxiHead.foreach(println)

// Parse data into geospatial class
import com.esri.core.geometry.Point
import com.github.nscala_time.time.Imports._
case class Trip(pickupTime: DateTime, dropoffTime: DateTime, pickupLoc: Point, dropoffLoc: Point)

val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
def point(longitude: String, latitude: String): Point = {
 new Point(longitude.toDouble, latitude.toDouble)
}

def parse(line: String): (String, Trip) = {
 val fields = line.split(',')
 val license = fields(1)
 val pickupTime = new DateTime(formatter.parse(fields(5)))
 val dropoffTime = new DateTime(formatter.parse(fields(6)))
 val pickupLoc = point(fields(10), fields(11))
 val dropoffLoc = point(fields(12), fields(13))
 val trip = Trip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
 (license, trip)
}

// Catch invalid records
def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
 new Function[S, Either[T, (S, Exception)]] with Serializable {
  def apply(s: S): Either[T, (S, Exception)] = {
   try {
    Left(f(s))
   } catch {
    case e: Exception => Right((s, e))
   }
  }
 }
}
val safeParse = safe(parse)
val taxiParsed = taxiRaw.map(safeParse)
taxiParsed.cache()

taxiParsed.map(_.isLeft).countByValue().foreach(println)
val taxiBad = taxiParsed.filter(_.isRight).map(_.right.get)
// or
val taxiBad = taxiParsed.collect({
 case t if t.isRight => t.right.get
})

  // drop bad records
val taxiGood = taxiParsed.collect({
 case t if t.isLeft => t.left.get
})
taxiGood.cache()

// Distribution of trip lengths (+ more cleaning)
import org.joda.time.Duration
def hours(trip: Trip): Long = {
 val d = new Duration(
  trip.pickupTime,
  trip.dropoffTime)
 d.getStandardHours
}
taxiGood.values.map(hours).countByValue().toList.sorted.foreach(println)

taxiGood.values.filter(trip => hours(trip) == -8).collect().foreach(println)

val taxiClean = taxiGood.filter{
 case (lic, trip) => {
  val hrs = hours(trip)
  (0<=hrs) && (hrs<3)
 }
}
// =>	// annoying formatting

// Examine dropoff boroughs
val geojson = scala.io.Source.fromFile("nyc-boroughs.geojson").mkString
import com.cloudera.datascience.geotime._
import GeoJsonProtocol._
import spray.json._
val features = geojson.parseJson.convertTo[FeatureCollection]

  // test functionality is as expected
val p = new Point(-73.994499, 40.75066)
val borough = features.find(f => f.geometry.contains(p))

  // improve efficiency by changing order of boroughs by area
val areaSortedFeatures = features.sortBy(f => {
 val borough = f("boroughCode").convertTo[Int]
 (borough, -f.geometry.area2D())
})
val bFeatures = sc.broadcast(areaSortedFeatures)

def borough(trip: Trip): Option[String] = {
 val feature: Option[Feature] = bFeatures.value.find(f => {
  f.geometry.contains(trip.dropoffLoc)
 })
 feature.map(f => {
  f("borough").convertTo[String]
 })
}
taxiClean.values.map(borough).countByValue().foreach(println)

  // check None trips
taxiClean.values.filter(t => borough(t).isEmpty).take(10).foreach(println)

  // filter out zeros
def hasZero(trip: Trip): Boolean = {
 val zero = new Point(0.0, 0.0)
 (zero.equals(trip.pickupLoc) || zero.equals(trip.dropoffLoc))
}
val taxiDone = taxiClean.filter {
 case (lic, trip) => !hasZero(trip)
}.cache()

  // rerun borough analysis
taxiDone.values.map(borough).countByValue().foreach(println)

// Build sessions
def secondaryKeyFunc(trip: Trip) = trip.pickupTime.getMillis
def split(t1: Trip, t2: Trip): Boolean = {
 val p1 = t1.pickupTime
 val p2 = t2.pickupTime
 val d = new Duration(p1, p2)
 d.getStandardHours >= 4
}

val sessions = groupByKeyAndSortValues(taxiDone, secondaryKeyFunc, split, 30)
sessions.cache()

// Analyse session data by calculating borough durations
def boroughDuration(t1: Trip, t2: Trip) = {
 val b = borough(t1)
 val d = new Duration(t1.dropoffTime, t2.pickupTime)
 (b, d)
}
val boroughDurations: RDD[(Option[String], Duration)] = sessions.values.flatMap(
 trips => {
  val iter: Iterator[Seq[Trip]] = trips.sliding(2)
  val viter = iter.filter(_.size == 2)
  viter.map(p => boroughDuration(p(0), p(1)))
}).cache()
boroughDurations.values.map(_.getStandardHours).countByValue().toList.sorted.foreach(println)

  // Remove negatives and calculate duration stats for each borough
import org.apache.spark.util.StatCounter
boroughDurations.filter {
 case (b, d) => d.getMillis >= 0
}.mapValues(d => {
 val s = new StatCounter()
 s.merge(d.getStandardSeconds)
}).reduceByKey((a, b) => a.merge(b)).collect().foreach(println)