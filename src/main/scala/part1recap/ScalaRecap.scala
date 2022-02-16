package part1recap

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}

object ScalaRecap {

  // value
  val aBoolean: Boolean = false
  var aVariable: Int = 56
  aVariable += 1

  // expressions
  val anIfExpression: String = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit: Unit = println("Hello, Scala") // Unit === "void"

  // OOP
  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  // inheritance: extends <= 1 class, but inherit from >= 0 traits
  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("eating this poor fellow")
  }

  // singleton
  object MySingleton

  // companions
  object Carnivore

  // case classes
  case class Person(name: String, age: Int)

  // generics
  class MyList[A] // can add variance modifiers - not important for this course

  // method notation
  // croc.eat(animal) OR croc eat animal
  val three = 1 + 2
  val three_v2 = 1.+(2)

  // FP
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(4) // 5, same as incrementer.apply(4)

  // map flatMap filter = HOFs
  val processedList = List(1,2,3).map(incrementer) // [2,3,4]
  val aLongerList = List(1,2,3).flatMap(x => List(x, x + 1)) // [1,2, 2,3, 3,4]

  // for-comprehensions
  val checkerboard = List(1,2,3).flatMap(n => List('a', 'b', 'c').map(c => (n, c)))
  val checkerboard_v2 = for {
    n <- List(1,2,3)
    c <- List('a', 'b', 'c')
  } yield (n, c) // same

  // options and try
  val anOption: Option[Int] = Option(/* something that might be null*/ 43)
  val doubleOption = anOption.map(_ * 2)

  val anAttempt: Try[Int] = Try(12)
  val modifiedAttempt = anAttempt.map(_ * 10)

  // pattern matching
  val anUnknown: Any = 45
  val medal = anUnknown match {
    case 1 => "gold"
    case 2 => "silver"
    case 3 => "bronze"
    case _ => "no medal"
  }

  val optionDescription = anOption match {
    case Some(value) => s"the option is not empty: $value"
    case None => "the option is empty"
  }

  // Futures
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
  val aFuture = Future(/* something to be evaluated on another thread*/ 1 + 41)

  // register callback when it finishes
  aFuture.onComplete {
    case Success(value) => println(s"the async meaning of life is $value")
    case Failure(exception) => println(s"the meaning of value failed: $exception")
  }

  val aPartialFunction: PartialFunction[Try[Int], Unit] = {
    case Success(value) => println(s"the async meaning of life is $value")
    case Failure(exception) => println(s"the meaning of value failed: $exception")
  }

  // map, flatMap, filter, ...
  val doubledAsyncMOL: Future[Int] = aFuture.map(_ * 2)

  // implicits

  // 1 - implicit arguments and values
  implicit val timeout: Int = 3000 // implicit val == given instance
  def setTimeout(f: () => Unit)(implicit tout: Int) = { // (using tout: Int)
    Thread.sleep(tout)
    f()
  }

  setTimeout(() => println("timeout")) // (timeout)

  // 2 - extension methods
  implicit class MyRichInt(number: Int) { // implicit class = extension
    def isEven: Boolean = number % 2 == 0
  }

  val is2Even = 2.isEven // new RichInt(2).isEven

  // 3 - conversions
  implicit def string2Person(name: String): Person =
    Person(name, 57)

  val daniel: Person = "Daniel" // string2Person("Daniel")

  def main(args: Array[String]): Unit = {

  }
}
