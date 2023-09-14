import kotlinx.coroutines.*
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.system.measureNanoTime
import kotlin.system.measureTimeMillis

suspend fun <TPos> query(docColl: SingleIndexDocumentCollection<TPos>, q: String) {
    println("querying for \"$q\" ...")
    lateinit var res: Map<Path, List<TPos>>
    val time = measureNanoTime { res = docColl.query(q) }
    val x = res.toList()
    val totalCount = x.sumOf { it.second.size }
    println(
        "total results for \"$q\": $totalCount in $time ns:\n\t${
            x.filter { it.second.isNotEmpty() }
                .joinToString(",\n\t") { "${it.first} (${it.second.size}): [${it.second.joinToString(", ")}]" }
        }"
    )
}

const val EXIT_COMMAND = "exit"
const val QUERY_COMMAND = "search "
const val REGISTER_COMMAND = "add "
const val UNREGISTER_COMMAND = "remove "

fun main() {
    runBlocking {
        coroutineScope {
            withContext(Dispatchers.Default) {
                SingleIndexDocumentCollection(CaseInsensitiveWordTokenizer(), this).use { docColl ->
                    var q = readln()
                    while (true) {
                        when {
                            q.startsWith(EXIT_COMMAND) -> break
                            q.startsWith(QUERY_COMMAND) -> query(docColl, q.replace(QUERY_COMMAND, ""))
                            q.startsWith(REGISTER_COMMAND) -> {
                                val path = q.replace(REGISTER_COMMAND, "")
                                when (val res = docColl.addWatch(Path(path))) {
                                    RegisterDirResult.Ok -> println("'$path' being watched now")
                                    RegisterDirResult.AlreadyWatched -> println("'$path' is already watched")
                                    is RegisterDirResult.Error -> println("'$path' watch encountered an error: ${res.ex}")
                                }
                            }
//
                            q.startsWith(UNREGISTER_COMMAND) -> {
                                val path = q.replace(UNREGISTER_COMMAND, "")
                                when (val res = docColl.removeWatch(Path(path))) {
                                    is UnregisterDirResult.Error -> println("'$path' unwatch encountered an error: ${res.ex}")
                                    UnregisterDirResult.Ok -> println("'$path' is not watched now")
                                    UnregisterDirResult.ParentWatched -> println("'$path''s parent is watched (so it would be watched)")
                                    UnregisterDirResult.WasNotWatched -> println("'$path' was not watched already")
                                }
                            }

                            else -> query(docColl, q)
                        }

                        q = readln()
                    }
                }
            }
        }
    }
}
