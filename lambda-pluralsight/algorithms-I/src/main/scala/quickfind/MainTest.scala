package quickfind

object MainTest extends App {

  val uf = new QuickFindUF(10)

  println(uf)
  println("connected : 1, 5 " + uf.connected(1, 5))
  uf.union(1, 5)
  println(uf)
  println("connected : 1, 5 " + uf.connected(1, 5))

  val uuf = new QuickUnionUF(10)
  println("----------------------------------------")

  println(uuf)
  println("connected : 1, 5 " + uuf.connected(1, 5))
  uuf.union(1, 5)
  println(uuf)
  println("connected : 1, 5 " + uuf.connected(1, 5))


}
