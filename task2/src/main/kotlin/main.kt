import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.UntypedAbstractActor
import akka.routing.Broadcast
import akka.routing.RoundRobinPool
import io.reactivex.rxjava3.core.BackpressureStrategy
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Scheduler
import io.reactivex.rxjava3.schedulers.Schedulers
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.math.BigInteger
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import kotlin.concurrent.thread
import kotlin.system.measureTimeMillis


fun startAndJoin(vararg blocks: () -> Unit) {
    val threads = blocks.map { thread(block = it, isDaemon = true, start = true) }
    threads.forEach { it.join() }
}

fun generateFile(fileName: String, seed: Long = 21) {
    val writer = File(fileName).writer()

    val random = Random(seed)
    for (i in 1..2000) {
        val randomBigInteger = BigInteger(48, random)
        writer.write(randomBigInteger.toString())
        writer.write("\n")
    }

    writer.close()
}

fun countPrimeMultipliers(number: BigInteger) : Int {
    var number = number.toLong()
    var count = 0
    var multiplier = 2L
    while (multiplier <= number) {
        if (number % multiplier == 0L) {
            count += 1
            number /= multiplier
        } else {
            multiplier += 1
        }
    }
    return count
}

fun processFileDirectly(fileName: String) : Int {
    var count = 0
    File(fileName).forEachLine {
        val bigInteger = BigInteger(it)
        count += countPrimeMultipliers(bigInteger)
    }
    return count
}

fun processFileWithMultithreading(fileName: String) : Int {
    var count = 0
    val workers = 3
    val file = File(fileName)

    val bq: BlockingQueue<String> = ArrayBlockingQueue<String>(workers)

    val producer = {
        file.forEachLine {
            bq.put((it))
        }
        // Consumer processes numbers so we can use non-number string for tell them
        // that File is over
        bq.put("<e>")
    }

    val consumer = {
        while (true) {
            val line = bq.take()
            if (line == "<e>") {
                bq.put("<e>")
                break
            } else {
                val sum = countPrimeMultipliers(BigInteger(line))
                count += sum
            }
        }
    }

    // Current PC got 4 logical threads. We`ll use 1 producer and 3 consumers
    startAndJoin(producer, consumer, consumer, consumer)

    return count
}

fun processFileWithRXKotlin(fileName: String) : Int {
    val count = Flowable.create<String>({ emitter ->
        File(fileName).forEachLine {
            emitter.onNext(it)
        }
        emitter.onComplete()
    }, BackpressureStrategy.BUFFER)  // not the best strategy, but the most suitable there
    .subscribeOn(Schedulers.computation())  // automatically uses number of threads presented on PC
    .map { line ->
        countPrimeMultipliers(BigInteger(line))
    }
    .reduce { t1, t2 ->  t1 + t2 }
    .blockingGet()

    return count
}

data class Line(val text: String)
data class Counted(val sum: Int)
data class FileName(val fileName: String)
class Start

class CountForOneLineActor: UntypedAbstractActor() {
    override fun onReceive(message: Any?) {
        when(message) {
            is Line -> {
                val count = countPrimeMultipliers(BigInteger(message.text))

                sender.tell(Counted(count), self)
            }
        }
    }
}

class MasterActor(): UntypedAbstractActor() {
    val numWorkers = 3
    val worker = context.actorOf(RoundRobinPool(numWorkers).props(Props.create(CountForOneLineActor::class.java)), "counter")
    var count = 0
    lateinit var reader: BufferedReader

    override fun onReceive(message: Any?) {
        when(message) {
            is FileName -> {
                reader = File(message.fileName).bufferedReader()
                for (i in 1..numWorkers) {
                    val line = reader.use { it.readLine() }
                    if (line != null) {
                        worker.tell(Line(line), self)
                    } else {
                        println(count)
                        context.system.terminate()
                    }
                }
            }
            is Counted -> {
                count += message.sum
                val line = reader.use { it.readLine() }
                if (line != null) {
                    worker.tell(Line(line), self)
                } else {
                    println(count)
                    context.system.terminate()
                }
            }
        }
    }
}

fun processFileWithAkka(fileName: String) {
    val system = ActorSystem.create("FileProcessSystem")

    val masterActor = system.actorOf(Props.create(MasterActor::class.java), "master")
    masterActor.tell(File(fileName), null)
}

fun main() {
    val fileName = "file.txt"

    generateFile(fileName)
    println("File is generated")

    val resultProcessFileDirectly: Int
    val timeInMillisDirectly = measureTimeMillis {
        resultProcessFileDirectly = processFileDirectly(fileName)
    }
    println(resultProcessFileDirectly)
    println("Direct processing took $timeInMillisDirectly ms")

    val resultProcessFileWithMultithreading: Int
    val timeInMillisMultithreading = measureTimeMillis {
        resultProcessFileWithMultithreading = processFileWithMultithreading(fileName)
    }
    println(resultProcessFileWithMultithreading)
    println("Multithreaded processing took $timeInMillisMultithreading ms")

    val resultProcessFileWithRXKotlin: Int
    val timeInMillisRXKotlin = measureTimeMillis {
        resultProcessFileWithRXKotlin = processFileWithRXKotlin(fileName)
    }
    println(resultProcessFileWithRXKotlin)
    println("RXKotlin processing took $timeInMillisRXKotlin ms")

    val timeInMillisAkka = measureTimeMillis {
        processFileWithAkka(fileName)
    }
    println(resultProcessFileWithRXKotlin)
    println("Akka processing took $timeInMillisAkka ms")
}