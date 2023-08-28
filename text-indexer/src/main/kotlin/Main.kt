import java.io.File
import kotlinx.coroutines.*


fun main(args: Array<String>) {
//    val path = File(args[0]);
//    println("path: $path")
//    GlobalScope.launch {
//        val watcherCh = path.asWatchChannel();
//        for (x in watcherCh) {
//            println(x)
//        }
//    }
//
//    println("press enter to exit")
//    readln()
    val f = File("test/война_и_мир.txt")
    val d = Document(f)
}