import scala.collection.BitSet

package object streams {
  type Hash_Function = String => Int
  type Standing_Query = (Vector[Int], Int) => Unit
}

package streams {

  case class Filter_Parms(
    nItemsSeen:Int, // number of items used to create the filter
    nBits:Int, // number of bits used in the filter
    nHashes:Int, // number of hashing functions used by the filter
    filter:BitSet   // the actual bitset
  )

  object utils  {

    private val PRIME =  4294967311L 
    private val N_HASHES = 100

    // seed the  random number generator, so we have reproducible results
    private val rand = new scala.util.Random(PRIME)

    @scala.annotation.tailrec
    private def k_random(k:Int) : List[Long] = {
      // return a vector of k random distinct integers
      val lst = for(i<- 1 to k) yield rand.nextLong()
      val l2 = lst.toSet
      val l3 = lst.toList
      // make sure it has exactly k elements
      // simply recurse until we get exactly k different ones
      // it has to happen at some point :)
      if (l2.size == k)
        l3
      else
        k_random(k)
    }

    private def generate_random_coefficients(k: Int) : List[Long] = {
      // generate k random coeffients without replacement
      k_random(k)
    }

    private def create_hash_functions(n:Int): List[Hash_Function] = {

      val aCoefs = generate_random_coefficients(n)
      val bCoefs = generate_random_coefficients(n)

      def hash_function(a:Long,b:Long)(st:String):Int = ((a * st.hashCode() + b) % utils.PRIME).toInt.abs

      aCoefs.zip(bCoefs).map{
        case (a,b) =>
          hash_function(a,b)(_)
      }
    }

    val hashes: List[Hash_Function] = create_hash_functions(N_HASHES)

    def truncate(v: Double, dec: Int) = {
      val factor = math.pow(10, dec)
      Math.floor(v * factor) / factor
    }
  }


}


