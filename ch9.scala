ssh -i SparkAA.pem hadoop@
wget https://github.com/sryza/aas/archive/1st-edition.zip
cd aas-1st-edition/ch09-risk
mvn package
cd data
./download-all-symbols.sh
mkdir factors
./download-symbol.sh SNP factors
./download-symbol.sh NDX factors
// Data is in ./stocks and ./factors directories
// Now need to copy paste data from Investing.com
  // http://www.investing.com/commodities/crude-oil-historical-data into factors/crudeoil.tsv
  // http://www.investing.com/rates-bonds/us-30-yr-t-bond-historical-data into factors/us30yeartreasurebonds.tsv
spark-shell

// Parse dates
import java.text.SimpleDateFormat
val format = new SimpleDateFormat("MMM d, yyyy")
format.parse("Oct 24, 2014")

// Read and process inputs locally (parallelisation only needed for compute)
  // Investing.com history
import com.github.nscala_time.time.Imports._
import java.io.File
import scala.io.Source
def readInvestingDotComHistory(file: File): Array[(DateTime, Double)] = {
 val format = new SimpleDateFormat("MMM d, yyyy")
 val lines = Source.fromFile(file).getLines().toSeq
 lines.map(line => {
  val cols = line.split('\t')
  val date = new DateTime(format.parse(cols(0)))
  val value = cols(1).toDouble
  (date, value)
 }).reverse.toArray
}

  // Yahoo! history
def readYahooHistory(file: File): Array[(DateTime, Double)] = {
 val format = new SimpleDateFormat("yyyy-MM-dd")
 val lines = Source.fromFile(file).getLines().toSeq
 lines.tail.map(line => {     // exclude header row
  val cols = line.split(',')
  val date = new DateTime(format.parse(cols(0)))
  val value = cols(1).toDouble
  (date, value)
 }).reverse.toArray
}

// Filter out instruments with <5 years history
val start = new DateTime(2009, 10, 23, 0, 0)
val end = new DateTime(2014, 10, 23, 0, 0)

val files = new File("data/stocks/").listFiles()
val rawStocks: Seq[Array[(DateTime, Double)]] = files.flatMap(file => {
 try {
  Some(readYahooHistory(file))
 } catch {
  case e: Exception => None
 }
}).filter(_.size >= 260*5+10)

val factorsPrefix = "data/factors/"
val factors1: Seq[Array[(DateTime, Double)]] =
 Array("crudeoil.tsv", "us30yeartreasurybonds.tsv")
  .map(x => new File(factorsPrefix + x))
  .map(readInvestingDotComHistory)
val factors2: Seq[Array[(DateTime, Double)]] =
 Array("SNP.csv", "NDX.csv")
  .map(x => new File(factorsPrefix + x))
  .map(readYahooHistory)

// Trim dates
def trimToRegion(history: Array[(DateTime, Double)],
 start: DateTime, end: DateTime): Array[(DateTime, Double)] = {
  var trimmed = history.dropWhile(_._1 < start).takeWhile(_._1 <= end)
  if (trimmed.head._1 != start) {
   trimmed = Array((start, trimmed.head._2)) ++ trimmed
  }
  if (trimmed.last._1 != end) {
   trimmed = trimmed ++ Array((end, trimmed.last._2))
  }
  trimmed
}

// Impute (fill down) missing data
import scala.collection.mutable.ArrayBuffer
def fillInHistory(history: Array[(DateTime, Double)],
 start: DateTime, end: DateTime): Array[(DateTime, Double)] = {
  var cur = history
  val filled = new ArrayBuffer[(DateTime, Double)]()
  var curDate = start
  while (curDate < end) {
   if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
    cur = cur.tail
   }
   filled += ((curDate, cur.head._2))
   curDate += 1.days
   // Skip weekends
   if (curDate.dayOfWeek().get > 5) curDate += 2.days
  }
  filled.toArray
}

val stocks: Seq[Array[(DateTime, Double)]] =
 rawStocks.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))
val factors: Seq[Array[(DateTime, Double)]] =
 (factors1 ++ factors2).map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))
(stocks ++ factors).forall(_.size == stocks(0).size)    // check all now equal length

// Determine factor weights with linear regression over 10-day (2-week) intervals
  // Create sliding intervals
def twoWeekReturns(history: Array[(DateTime, Double)]) : Array[Double] = {
 history.sliding(10)
 .map(window => window.last._2 - window.head._2)
 .toArray
}
val stocksReturns = stocks.map(twoWeekReturns)
val factorsReturns = factors.map(twoWeekReturns)

  // Transpose matrix for modelling
def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
 val mat = new Array[Array[Double]](histories.head.length)
 for (i <- 0 until histories.head.length) {
  mat(i) = histories.map(_(i)).toArray
 }
 mat
}
val factorMat = factorMatrix(factorsReturns)

  // Add additional features (square roots and squares)
def featurize(factorReturns: Array[Double]): Array[Double] = {
 val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
 val squareRootedReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
 squaredReturns ++ squareRootedReturns ++ factorReturns
}
val factorFeatures = factorMat.map(featurize)

  // Fit linear models
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]]): OLSMultipleLinearRegression = {
 val regression = new OLSMultipleLinearRegression()
 regression.newSampleData(instrument, factorMatrix)
 regression
}
val models = stocksReturns.map(linearModel(_, factorFeatures))

val factorWeights = models.map(_.estimateRegressionParameters()).toArray

// Sampling, kde and plotting
import com.cloudera.datascience.risk.KernelDensity
import breeze.plot._
def plotDistribution(samples: Array[Double]) {
 val min = samples.min
 val max = samples.max
 val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
 val densities = KernelDensity.estimate(samples, domain)
 val f = Figure()
 val p = f.subplot(0)
 p += plot(domain, densities)
 p.xlabel = "Two Week Return ($)"
 p.ylabel = "Density"
}
plotDistribution(factorReturns(0))
plotDistribution(factorReturns(1))

# http://stackoverflow.com/questions/6615489/fitting-distributions-goodness-of-fit-p-value-is-it-possible-to-do-this-with/16651524#16651524

  // Check correlations
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
val factorCor = new PearsonsCorrelation(factorMat).getCorrelationMatrix().getData()
println(factorCor.map(_.mkString("\t")).mkString("\n"))

// Fit multivariate normal distribution
import org.apache.commons.math3.stat.correlation.Covariance
val factorCov = new Covariance(factorMat).getCovarianceMatrix().getData()
val factorMeans = factorsReturns.map(factor => factor.sum / factor.size).toArray
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
val factorsDist = new MultivariateNormalDistribution(factorMeans, factorCov)
factorsDist.sample()
factorsDist.sample()

// Run trials
  // Partition along instruments (not used here, skeleton code)
val randomSeed = 1496
val instrumentsRdd = ...
def trialLossesForInstrument(seed: Long, instrument: Array[Double]): Array[(Int, Double)] = {
 ...
}
instrumentsRdd.flatMap(trialLossesForInstrument(randomSeed, _)).reduceByKey(_ + _)

  // Partition along trials
    // Different seed in each partition
val parallelism = 1000
val baseSeed = 1496
val seeds = (baseSeed until baseSeed + parallelism)
val seedRdd = sc.parallelize(seeds, parallelism)

    // Generate trial parameters and calculate returns
def instrumentTrialReturn(instrument: Array[Double], trial: Array[Double]): Double = {
 var instrumentTrialReturn = instrument(0)
 var i=0
 while (i < trial.length) {
  instrumentTrialReturn += trial(i) * instrument(i+1)
  i+=1
 }
 instrumentTrialReturn
}

def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]): Double = {
 var totalReturn = 0.0
 for (instrument <- instruments) {
  totalReturn += instrumentTrialReturn(instrument, trial)
 }
 totalReturn
}

    // Apply random number generator and apply samples to model
import org.apache.commons.math3.random.MersenneTwister
def trialReturns(seed: Long, numTrials: Int, instruments: Seq[Array[Double]],
 factorMeans: Array[Double], factorCovariances: Array[Array[Double]]): Seq[Double] = {
  val rand = new MersenneTwister(seed)
  val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans, factorCovariances)
  val trialReturns = new Array[Double](numTrials)
  for (i <- 0 until numTrials) {
   val trialFactorReturns = multivariateNormal.sample()
   val trialFeatures = featurize(trialFactorReturns)
   trialReturns(i) = trialReturn(trialFeatures, instruments)
  }
  trialReturns
}

    // Run trials - factor weights here used as instrument to weight trial factors
val numTrials = 10000000
val bFactorWeights = sc.broadcast(factorWeights)
val trials = seedRdd.flatMap( trialReturns(_, numTrials / parallelism,
 bFactorWeights.value, factorMeans, factorCov))

// Find (Conditional) Value at Risk
def fivePercentVaR(trials: RDD[Double]): Double = {
 val topLosses = trials.takeOrdered(math.max(trials.count().toInt / 20, 1))
 topLosses.last
}
val valueAtRisk = fivePercentVaR(trials)

def fivePercentCVaR(trials: RDD[Double]): Double = {
 val topLosses = trials.takeOrdered(math.max(trials.count().toInt / 20, 1))
 topLosses.sum / topLosses.length
}
val conditionalValueAtRisk = fivePercentVaR(trials)

// Visualise returns distribution (same as above)
def plotDistribution(samples: RDD[Double]) {
 val stats = samples.stats()
 val min = stats.min
 val max = stats.max
 val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
 val densities = KernelDensity.estimate(samples, domain)
 val f = Figure()
 val p = f.subplot(0)
 p += plot(domain, densities)
 p.xlabel = "Two Week Return ($)"
 p.ylabel = "Density"
}
plotDistribution(trials)

// Evaluate results - confidence intervals and chi-squared tests
def bootstrappedConfidenceInterval(trials: RDD[Double], computeStatistic: RDD[Double] => Double,
 numResamples: Int, pValue: Double): (Double, Double) = {
  val stats = (0 until numResamples).map { i =>
   val resample = trials.sample(true, 1.0)
   computeStatistic(resample)
  }.sorted
  val lowerIndex = (numResamples * pValue / 2).toInt
  val upperIndex = (numResamples * (1 - pValue / 2)).toInt
  (stats(lowerIndex), stats(upperIndex))
}
bootstrappedConfidenceInterval(trials, fivePercentVaR, 100, .05)
bootstrappedConfidenceInterval(trials, fivePercentCVaR, 100, .05)


var failures = 0
for (i <- 0 until stocksReturns(0).size) {
 val loss = stocksReturns.map(_(i)).sum
 if (loss < valueAtRisk) {
  failures += 1
 }
}
failures

val failureRatio = failures.toDouble / total
val logNumer = (total - failures) * math.log1p(-confidenceLevel) + failures * math.log(confidenceLevel)
val logDenom = (total - failures) * math.log1p(-failureRatio) + failures * math.log(failureRatio)
val testStatistic = -2 * (logNumer - logDenom)

import org.apache.commons.math3.distribution.ChiSquaredDistribution
1 - new ChiSquaredDistribution(1.0).cumulativeProbability(testStatistic)
