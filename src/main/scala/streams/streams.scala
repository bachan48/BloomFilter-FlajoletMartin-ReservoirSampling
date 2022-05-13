import scala.collection.mutable.BitSet

package streams {

  /*
  Student Name: Bachan Ghimire
  ID: V00996378
  Program: MSc
   */

  class Bloom_Filter (fileName: String,
    falsePositiveRate: Double,
    hashes: List[Hash_Function]) {

    val bloomFilter : BitSet = BitSet()
    val lines =   io.Source.fromFile(fileName).getLines()

    //count lines in the input file assuming input file is unique
    val numberItemsN:Int = lines.foldLeft(0){(counter:Int, x:String) => {
        counter+1
      }
    }

    val falsePositiveP = falsePositiveRate
    val bitSizeM = math.ceil(-1 * numberItemsN * math.log(falsePositiveP) / math.log(2) / math.log(2)).toInt
    val numHashesK = math.ceil(bitSizeM / numberItemsN * math.log(2)).toInt
    println(bitSizeM, numHashesK)
    //hash functions to use from available hashes, pre-condition so that there's no runtime exception.
    val hashFunctionsInUse = if (numHashesK < utils.hashes.size) hashes.take(numHashesK) else hashes

    def parameters(): Filter_Parms = {
      val lines =  io.Source.fromFile(fileName).getLines()
      for (item <- lines){
        for (function <- hashFunctionsInUse){
          val hashedValue = function(item)
          bloomFilter.add(hashedValue % bitSizeM)
        }
      }
      Filter_Parms(numberItemsN,bitSizeM,numHashesK,bloomFilter)
    }

    def in(v:String):Boolean = {

      for(function <- hashFunctionsInUse ){ //iterate through hashes
        val hashedValue = function(v)
        if(!bloomFilter(hashedValue % bitSizeM))
          return false //if either of the hashed bit is not found, return false
      }
      true //if every single hashed bit matches, only then return true
    }

  }
 
  class Flajolet_Martin(
    s : Iterator[String],
    bits: Int,
    hashes: List[Hash_Function]) {

    val hashCounts: List[Int] = {

      val trailingZerosList: Array[Array[Int]] = s.map(item=>{

        val trailingZerosPerHash = hashes.map(function =>{
          val hashedResult = function(item)
          val trailingZerosCounter: Int= {
            val mask = (1 << bits) - 1 //mask the bits to ignore
            val nBits = hashedResult & mask
            val count = {
              if (nBits % 2 == 1) 0
              else if (nBits == 0) nBits //nBits ignores leading 0s, so n-0s always = 0 nBits
              else countTrailingZero(nBits, 0)  //initial count = 0
            }
            count
          }
          trailingZerosCounter
        }).toArray

        trailingZerosPerHash
      }).toArray

      /*
        The following code will zip available hashes with index
        For each array above, it will take only the value in current index and return the max value
        This does not load anything to memory expect for that max int, thus making it highly efficient
        Each max is raised as power of 2, and the counts are returned as a List[Int], satisfying the requirement
       */

      val distinctRecordsCounts: List[Int] = hashes.zipWithIndex.map(hash=>{
        val hashIndex: Int = hash._2
        val maxOfAllHash: Int = trailingZerosList.map{_(hashIndex)}.max //get the highest #0 count for each hash function
        val distinctCount_R = math.pow(2, maxOfAllHash).toInt
        distinctCount_R
      })

      distinctRecordsCounts
    }

    def countTrailingZero(number: Int, count:Int): Int = {
      if ((number & 1) == 0 ) {
        countTrailingZero(number >> 1, count+1)
      }
      else count
    }

    def summarize(groupSize:Int):Double = {
      val hashCountsBucket = hashCounts.sorted.grouped(groupSize)
      val medianBuckets = hashCountsBucket.map(listOfR => {   //median of counts in buckets
        if (listOfR.size % 2 == 1)
          listOfR(listOfR.size / 2) //get median element
        else {
          val (firstHalf, secondHalf) = listOfR.splitAt(listOfR.size / 2)
          (firstHalf.last + secondHalf.head) / 2 //average of 2 median elements
        }
      }).toList

      val approximation = medianBuckets.sum / medianBuckets.length    //average of medians from buckets
      approximation
    }
  }

  object reservoirSample {
  
    def process(
      s : Iterator[Int],
      sizeSampleK: Int,
      r: scala.util.Random,
      queries: List[Standing_Query]
    )  = {

      val (initialSample, streamContinued) = s.zipWithIndex.splitAt(sizeSampleK)
      val currentSample = initialSample.map(_._1).toVector

      val processes = streamContinued.filter(item => r.nextDouble() < sizeSampleK.toDouble / item._2.toDouble)
      .foldLeft(sizeSampleK: Int) { (streamPositionI: Int, streamItem: (Int, Int)) =>
        {
          val updatedSample = streamItem match {
            case (item, _) =>
              val replacementIndex = r.nextInt(sizeSampleK)
              if (sizeSampleK == streamPositionI)
                currentSample //return 1st instance of sample initially
              else currentSample.updated(replacementIndex, item) //return updated sample
          }
          //process query functions for each updated sample
          for (query <- queries) {
            query(updatedSample, streamPositionI)
          }
          streamPositionI + 1
        }
      }
      processes
    }

  }

}