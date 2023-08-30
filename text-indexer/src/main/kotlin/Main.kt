import java.io.File
import kotlinx.coroutines.*
import java.lang.Exception

fun main(args: Array<String>) {
    runBlocking {
        val f = File("test/война_и_мир.txt")
        val d = Document(f)
        launch(Dispatchers.Default) {
            try {
                println("query")
                val r = d.queryString("неожиданно")
                println("${r.size}, $r")
            } catch (e: Exception) {
                println(e)
            }

        }

        while (true) {
            val x = readln()
            println("Rebuilding")
            d.rebuildIndex()
        }
    }
}