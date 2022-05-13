package streams

object CountingMain extends App {

  //EXAMPLE SBT COMMAND: runMain streams.CountingMain "Data/4096.ints" 16 30

  //process arguments

  if (args.size < 3) {
    args.foreach(println)
    throw new  IllegalArgumentException(s"Arguments missing <filename> <bits> <Group Size>")
  }

  val fileName = args(0) //1st argument: filename
  val bits = args(1).toInt //2nd argument: bits to use
  val groupSize = args(2).toInt //3rd argument: Group Size

  if (bits < 1 || bits > 32) {
    throw new  IllegalArgumentException(s"Bits must be between 1 - 32")
  }

  if (groupSize < 2 || groupSize >100) {
    throw new  IllegalArgumentException(s"Group Size must be between 2 - 100")
  }

  val lines =   io.Source.fromFile(fileName).getLines()

  val totalAvailableHashes = utils.hashes.length
  //the number of hashes will be a multiple of the groupSize
  val numberOfHashesToUse = (totalAvailableHashes - (totalAvailableHashes % groupSize))

  val  flajoletMartin = new Flajolet_Martin(lines, bits, utils.hashes.take(numberOfHashesToUse))
  val result = flajoletMartin.summarize(groupSize)

  println(s"Approximate Distinct Records in the stream: $result")

}
