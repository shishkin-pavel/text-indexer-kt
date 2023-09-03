import kotlinx.coroutines.*
import kotlin.io.path.Path

fun main(args: Array<String>) {

    runBlocking {
        if (args.size != 1) {
            println("usage: <binary> <path to watched directory>")
        }
        val docColl =
            DocumentCollection(
                Path(args[0]),
                SimpleWordTokenizer(),
                { CharIndex() },    // TODO looks dirty, is there better way like C# 'new generic type constraint' or Rust "static" trait members?
                CoroutineScope(Dispatchers.Default)
            )

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