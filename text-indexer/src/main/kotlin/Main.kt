import kotlinx.coroutines.*
import kotlin.io.path.Path

fun main(args: Array<String>) {

    runBlocking {
        val rootPath = Path(args[0])
        val docColl = DocumentCollection(rootPath, CoroutineScope(Dispatchers.Default))
        var q = readln()
        while (q != "exit") {
            val res = docColl.query(q)
            val totalCount = res.sumOf { it.second.size }
            println("total results: $totalCount")

            for ((filePath, positions) in res) {
                if (positions.isNotEmpty()) {
                    println("$filePath, ${positions.size} results:")
                    val posStr = positions.joinToString(", ") { "${it.line}:${it.shift}" }
                    println(posStr)
                }
            }

            q = readln()
        }
    }
}