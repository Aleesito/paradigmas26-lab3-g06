import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonParser {

  /**
   * Parse Reddit JSON feed and extract posts.
   * @param jsonContent JSON string from Reddit API
   * @param subscriptionName name of subscription (for logging)
   * @return (list of posts/empty list if parsing fails, failed parse count)
   */
  def parsePosts(jsonContent: String, subscriptionName: String): List[Post] = {
    try {
      implicit val formats: Formats = DefaultFormats
      val json     = parse(jsonContent)
      val children = (json \ "data" \ "children").extract[List[JValue]]

      val results = children.map { child =>
        try {
          val data     = child \ "data"
          val title    = (data \ "title").extract[String]
          val selftext = (data \ "selftext").extract[String]
          Some(Post(title, selftext))
        } catch {
          case _: Exception => None
        }
      }

      val posts  = results.flatten
      posts

    } catch {
      case _: Exception =>
        Console.err.println(s"Warning: Failed to parse JSON from '$subscriptionName'")
        List.empty[Post]
    }
  }
}
