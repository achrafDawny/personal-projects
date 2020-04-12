package quickfind

class QuickUnionUF(n: Int) {
  val id = new Array[Int](n)
  for (i <- 0 to n - 1){
    id(i) = i
  }

  def root(i: Int): Int = {
    if(id(i) == i)
      i
    else
      root(id(i))
  }

  def connected(p: Int, q: Int): Boolean = {
    root(p) == root(q)
  }
  def union(p: Int, q: Int) = {
    id(root(p)) = root(q)
  }


}
