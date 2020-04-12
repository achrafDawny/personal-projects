package quickfind

class QuickFindUF(n: Int) {
  val id = new Array[Int](n)
  for(i <- 0 to n-1){
    id(i) = i
  }

  def connected (p: Int, q: Int): Boolean = {
    id(p) == id(q)
  }

  def union(p: Int, q: Int) = {
    val pid = id(p)
    val qid = id(q)
    var i = 0
    for (n <- id.iterator){
      if(n == pid)
        id(i) = qid
      i += 1
    }
  }
  override def toString: String = id.mkString(", ")
}
