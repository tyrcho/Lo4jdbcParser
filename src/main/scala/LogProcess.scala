import java.io.FileWriter
import scala.io.Codec
import java.io.File

object LogProcess extends App {

  case class Config(
    input: String = "",
    output: String = "",
    codec: String = "UTF-8",
    top: Int = 10)

  val parser = new scopt.OptionParser[Config]("logparser") {
    arg[String]("[input_file]") action { (x, c) => c.copy(input = x) }
    arg[String]("[output_file]") action { (x, c) => c.copy(output = x) }
    opt[String]('c', "codec") action { (x, c) => c.copy(codec = x) }
    opt[Int]('t', "top") action { (x, c) => c.copy(top = x) }
  }

  parser.parse(args, Config()) match {
    case Some(config) =>

      val codec = Codec(config.codec)
      val lines = joinLines(io.Source.fromFile(config.input)(codec).getLines)
      val out = new FileWriter(config.output)
      val topCount = config.top

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

      for ((key, req) <- requests.toList.groupBy(_.key).toList.sortBy(order)) {
        val count = req.map(_.count).sum
        println(s"$count $key requests")
        val total = req.map(_.time).sum / 1000
        println(s"total time : $total seconds")
        println(s"-- TOP $topCount requests, by time --")

        req.sortBy(-_.time).take(topCount).foreach(println)

        println(s"-- TOP $topCount requests, by total time for same requests --")
        val distinct = for ((r, entries) <- req.groupBy(_.request)) yield Entry(key, entries.map(_.time).sum, r, entries.map(_.count).sum)
        distinct.toList.sortBy(-_.time).take(topCount).foreach(println)

        println(s"-- TOP $topCount requests, by total time for similar (same statement, different values) requests --")
        val unique = for ((r, entries) <- req.groupBy(_.uniquePart)) yield Entry(key, entries.map(_.time).sum, entries.head.uniquePart, entries.map(_.count).sum)
        unique.toList.sortBy(-_.time).take(topCount).foreach(println)

        println()
      }
    case None => // arguments are bad, error message will have been displayed
  }

  def order[T](elt: (String, T)): Int = {
    val key = elt._1
    key match {
      case "select" => -4
      case "insert" => -3
      case "update" => -2
      case "delete" => -1
      case _        => key.head.toInt
    }
  }

  def lineToEntry(l: String) = {
    val simpleRe = """.* - .*\. (\w+) (.*) \{executed in (\d+) ms\}""".r
    val batchRe = """.* - .*\. batching (\d*) statements: \d*: (\w+) (.*) \{executed in (\d+) ms\}""".r
    l.toLowerCase match {
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