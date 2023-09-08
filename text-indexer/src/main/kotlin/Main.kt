import kotlinx.coroutines.*
import kotlin.io.path.Path
import kotlin.system.exitProcess

suspend fun <TPos> query(docColl: DocumentCollection<TPos>, q: String) {
    val res = docColl.query(q)
    val totalCount = res.sumOf { it.second.size }
    println("total results: $totalCount")

    for ((filePath, positions) in res) {
        if (positions.isNotEmpty()) {
            println("$filePath, ${positions.size} results:")
            val posStr = positions.joinToString(", ")
            println(posStr)
        }
    }
}

const val EXIT_COMMAND = "exit"
const val QUERY_COMMAND = "search "

fun main(args: Array<String>) {

    runBlocking {
        if (args.size != 1) {
            println("usage: <binary> <path to watched directory>")
            exitProcess(1)
        }
        val docColl =
            DocumentCollection(
                Path(args[0]),
                CaseInsensitiveWordTokenizer(),
                { CharIndex() },    // TODO looks dirty, is there better way like C# 'new generic type constraint' or Rust "static" trait members?
                CoroutineScope(Dispatchers.Default)
            )

        var q = readln()
        while (true) {
            when {
                q.startsWith(EXIT_COMMAND) -> break
                q.startsWith(QUERY_COMMAND) -> query(docColl, q.replace(QUERY_COMMAND, ""))
                else -> query(docColl, q)
            }

            q = readln()
        }
    }
}