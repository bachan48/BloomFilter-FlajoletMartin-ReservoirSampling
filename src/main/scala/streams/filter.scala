package streams

object FilterMain extends App {

  //EXAMPLE SBT COMMAND: runMain streams.FilterMain "Data/who-movies.txt" 0.1 "Data/test-movies.txt"

  //process arguments

  if (args.size < 3) {
    args.foreach(println)
    throw new  IllegalArgumentException(s"Arguments missing <filename> <False Positive Probability> <Search query file name>")
  }
  val fileName = args(0) //1st argument: filename
  val fpp = args(1).toDouble //2nd argument: False Positive Probability, between 0-1
  val queryFileName = args(2) //3rd argument: Search query - what to search for in the bitset/dataset

  if (fpp < 0 || fpp> 1) {
    throw new  IllegalArgumentException(s"Probability of False Positive should be between 0-1, ([${fpp}] provided)")
  }

  val bloomFilter = new Bloom_Filter(fileName, fpp, utils.hashes)
  bloomFilter.parameters()

  val queryLines =   io.Source.fromFile(queryFileName).getLines()
  for(query <- queryLines){
    val result =  bloomFilter.in(query)
    println(s"$query already exists : $result.")
  }

}
