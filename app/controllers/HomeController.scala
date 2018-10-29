package controllers
import javax.inject._
import models._
import play.api.mvc._

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents,configIntitaion: SparkConfigIntitaion,connInit: SparkConnInit) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }


  def toStartSession = Action {
    val csst = configIntitaion.toCustomHive("consolidationsalesstage1")
    val fmpt = configIntitaion.toStartSparkHive("fastmoveproductsstage1")
    //connInit.loadFastMoveProducts(ss)
    val result = connInit.productAnalysis(csst,fmpt)
    //val json = result.toJSON.collect().mkString
    Ok(result.toJSON.collect().mkString)
  }


















}
