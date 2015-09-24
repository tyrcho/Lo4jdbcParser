import java.io.FileWriter
import scala.io.Codec

object LogProcess extends App {
  if (args.size < 2) {
    System.err.println("Arguments : input_file output_file [Codec]")
    System.exit(1)
  }
  val (input, output) = (args(0), args(1))
  val codec = if (args.size < 2) Codec.UTF8 else Codec(args(2))
  val lines = joinLines(io.Source.fromFile(input)(codec).getLines)
  val out = new FileWriter(output)

  def println(a: Any) = {
    System.out.println(a)
    out.write(a.toString)
    out.write("\n")
    out.flush()
  }

  val requests = (for {
    l <- lines
    e <- lineToEntry(l)
  } yield e).toList

  val total = requests.map(_.count).sum
  println(s"$total Total requests")

  def order[T](elt: (String, T)): Int = {
    val key = elt._1
    if (key.toLowerCase == "select") -1 
    else key.head.toInt
  }

  for ((key, req) <- requests.toList.groupBy(_.key).toList.sortBy(order)) {
    val count = req.map(_.count).sum
    println(s"$count $key requests")
    val total = req.map(_.time).sum / 1000
    println(s"total time : $total seconds")
    req.sortBy(-_.time).take(10).foreach(println)

    println("--SAME--")
    val distinct = for ((req, entries) <- req.groupBy(_.request)) yield Entry(key, entries.map(_.time).sum, req, entries.map(_.count).sum)
    distinct.toList.sortBy(-_.time).take(10).foreach(println)

    println("--SIMILAR--")
    val unique = for ((req, entries) <- req.groupBy(_.uniquePart)) yield Entry(key, entries.map(_.time).sum, req, entries.map(_.count).sum)
    unique.toList.sortBy(-_.time).take(10).foreach(println)

    println()
  }

  def lineToEntry(l: String) = {
    val simpleRe = """.* - .*\. (\w+) (.*) \{executed in (\d+) ms\}""".r
    val batchRe = """.* - .*\. batching (\d*) statements: \d*: (\w+) (.*) \{executed in (\d+) ms\}""".r
    l match {
      case batchRe(count, key, request, time) => Some(Entry(key, time.toInt, request, count.toInt))
      case simpleRe(key, request, time)       => Some(Entry(key, time.toInt, request))
      case _                                  => None
    }
  }

  def joinLines(lines: Iterator[String]) = new Iterator[String] {
    def hasNext = lines.hasNext
    def next = {
      lines.takeWhile(_ != "").mkString
    }
  }

  case class Entry(key: String, time: Int, request: String, count: Int = 1) {
    override def toString =
      s"$count $key ($time ms) : $request"

    lazy val uniquePart = {
      val sep = key match {
        case "insert"            => " values "
        case "select" | "delete" => " where "
        case "update"            => "="
        case _                   => ""
      }
      if (request.contains(sep)) request.substring(0, request.indexOf(sep))
      else request
    }
  }
}