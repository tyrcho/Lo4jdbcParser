case class Config(
  input: String = "",
  output: String = "",
  codec: String = "UTF-8",
  top: Int = 10)

object Config {
  val parser = new scopt.OptionParser[Config]("logparser") {
    arg[String]("[input_file]") action { (x, c) => c.copy(input = x) }
    arg[String]("[output_file]") action { (x, c) => c.copy(output = x) }
    opt[String]('c', "codec") action { (x, c) => c.copy(codec = x) }
    opt[Int]('t', "top") action { (x, c) => c.copy(top = x) }
  }

  def parse(args: Array[String]) =
    parser.parse(args, Config())
}