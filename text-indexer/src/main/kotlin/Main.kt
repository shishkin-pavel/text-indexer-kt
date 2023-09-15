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
const val ADD_WATCH_COMMAND = "add "
const val REMOVE_WATCH_COMMAND = "remove "

fun main() {
    runBlocking {
        coroutineScope {
            withContext(Dispatchers.Default) {
                SingleIndexDocumentCollection(CaseInsensitiveWordTokenizer(), this).use { docColl ->
                    var q = readln()
                    while (true) {
                        when {
                            q.startsWith(EXIT_COMMAND) -> break
                            q.startsWith(QUERY_COMMAND) -> launch { query(docColl, q.replace(QUERY_COMMAND, "")) }
                            q.startsWith(ADD_WATCH_COMMAND) -> {
                                val path = q.replace(ADD_WATCH_COMMAND, "")

                                try {
                                    launch {
                                        val time = measureTimeMillis {
                                            val (resD, indexD) = docColl.addWatch(Path(path))
                                            when (val res = resD.await()) {
                                                RegisterDirResult.Ok -> println("'$path' being watched now")
                                                RegisterDirResult.AlreadyWatched -> println("'$path' is already watched")
                                                is RegisterDirResult.Error -> println("'$path' watch encountered an error: ${res.ex}")
                                            }
                                            indexD.await()
                                        }
                                        println("Indexing finished! it took $time ms since watch add (+ on-line file watches and other watch changes)")
                                    }

                                } catch (_: Exception) {
                                }
                            }
//
                            q.startsWith(REMOVE_WATCH_COMMAND) -> {
                                val path = q.replace(REMOVE_WATCH_COMMAND, "")
                                try {
                                    launch {
                                        val time = measureTimeMillis {
                                            val (resD, indexD) = docColl.removeWatch(Path(path))
                                            when (val res = resD.await()) {
                                                is UnregisterDirResult.Error -> println("'$path' unwatch encountered an error: ${res.ex}")
                                                UnregisterDirResult.Ok -> println("'$path' is not watched now")
                                                UnregisterDirResult.ParentWatched -> println("'$path''s parent is watched (so it would be watched)")
                                                UnregisterDirResult.WasNotWatched -> println("'$path' was not watched already")
                                            }
                                            indexD.await()
                                        }
                                        println("Indexing finished! it took $time ms since watch remove (+ on-line file watches and other watch changes)")
                                    }
                                } catch (_: Exception) {
                                }
                            }

                            else -> launch { query(docColl, q) }
                        }

                        q = readln()
                    }
                }
            }
        }
    }
}
