package quickfind

class UF(n: Int) {
  val id = new Array[Int](n)
  val sz = new Array[Int](n)
  for (i <- 0 to n - 1){
    id(i) = i
  }

  def root(i: Int): Int = {
    if(id(i) == i)
      i
    else {
      id(i) = id(id(i))
      root(id(i))
    }
  }

  def connected(p: Int, q: Int): Boolean = {
    root(p) == root(q)
  }
  def union(p: Int, q: Int) = {
    val i = root(p)
    val j = root(q)
    if(i != j)
      if(sz(i) < sz(j)) {
        id(i) = j
        sz(j) += sz(i)
      }
      else{
        id(j) = i
        sz(i) = sz(j)
      }

    id(root(p)) = root(q)
  }

}
