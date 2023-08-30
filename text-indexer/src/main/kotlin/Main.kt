//import java.io.File
//import kotlinx.coroutines.*
//import java.lang.Exception
//
//fun main(args: Array<String>) {
////    runBlocking {
////        val f = File("test/война_и_мир.txt")
////        val d = Document(f)
////        launch(Dispatchers.Default) {
////            try {
////                println("query")
////                val r = d.queryString("неожиданно")
////                println("${r.size}, $r")
////            } catch (e: Exception) {
////                println(e)
////            }
////
////        }
////
////        while (true) {
////            val x = readln()
////            println("Rebuilding")
////            d.rebuildIndex()
////        }
////    }
//
//    runBlocking {
//        val path = File(args[0]);
//        println("path: $path")
//        launch(Dispatchers.Default) {
//            val watcherCh = path.asWatchChannel();
//            for (x in watcherCh) {
//                println(x)
//            }
//        }
//
//        println("press enter to exit")
//        readln()
//    }
//}

import java.nio.file.*


fun main() {
    val dir = Paths.get("test")
    val fileWatcher = FileWatcher()
    fileWatcher.watchDirectoryTree(dir, false)
}