
import dispatch._, Defaults._
import scala.util.{Success, Failure}

object DispatchTest {

def main (args: Array[String]) {
  

     val svc = url("http://api.bigdatadev.pitneycloud.com:80/fusion/geocode/"+"7219 Greenleaf Ave".replace(" ", "%20")+"%2C"+""+"%2C"+"Cleveland"+"%2C"+"OH"+"%2C"+"44130-5022"+"%2C"+"usa".replace(" ", "%20")).as_!("pbuser", "hackathon")
    val response : Future[String] = Http.configure(_ setFollowRedirects true)(svc OK as.String)

    response onComplete {
      case Success(content) => {
        println("Successful response" + content)
      }
      case Failure(t) => {
        println("An error has occured: " + t.getMessage)
      }
    }
  }
}