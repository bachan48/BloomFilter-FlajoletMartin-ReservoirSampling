package streams

object SampleMain extends App {

  //EXAMPLE SBT COMMAND: runMain streams.SampleMain "Data/movies.ints" 1000

  //process arguments

  if (args.size < 2) {
    args.foreach(println)
    throw new  IllegalArgumentException(s"Arguments missing <filename> <sample size>")
  }
  val fileName = args(0) //1st argument: filename
  val sampleSize = args(1).toInt //2nd argument: sample size

  val lines =   io.Source.fromFile(fileName).getLines()

  val avg = (currentSample: Vector[Int], currentStreamNumber: Int) => {
    val avg = currentSample.sum/currentSample.length
    println(s"Average of sample: $avg at stream position $currentStreamNumber")
  }

  val sum = (currentSample: Vector[Int], currentStreamNumber: Int) => {
    val sum = currentSample.sum
    println(s"Sum of items in sample: $sum at stream position $currentStreamNumber")
  }

  val max = (currentSample: Vector[Int], currentStreamNumber: Int) => {
    val max = currentSample.max
    println(s"Largest item of sample: $max at stream position $currentStreamNumber")
    println("---------------")
  }

  val queries : List[Standing_Query] = List(avg, sum, max)
  val processes = reservoirSample.process(lines.map(_.toInt), sampleSize, scala.util.Random, queries)
  println(s"$processes streams items processed.")
}