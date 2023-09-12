import kotlinx.coroutines.*
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.system.measureNanoTime

suspend fun <TPos> query(docColl: DocumentCollection<TPos>, q: String) {
    println("querying for \"$q\" ...")
    lateinit var res: List<Pair<Path, ArrayList<TPos>>>
    val time = measureNanoTime { res = docColl.query(q) }
    val totalCount = res.sumOf { it.second.size }
    println(
        "total results for \"$q\": $totalCount in $time ns:\n${
            res.filter { it.second.isNotEmpty() }.joinToString(",\n") { "${it.first} (${it.second.size})" }
        }"
    )

//    for ((filePath, positions) in res) {
//        if (positions.isNotEmpty()) {
//            println("$filePath, ${positions.size} results:")
//            val posStr = positions.joinToString(", ")
//            println(posStr)
//        }
//    }
}

const val EXIT_COMMAND = "exit"
const val QUERY_COMMAND = "search "
const val REGISTER_COMMAND = "add "
const val UNREGISTER_COMMAND = "remove "

fun main(args: Array<String>) {
    runBlocking {
        coroutineScope {
            withContext(Dispatchers.Default) {
                DocumentCollection(
                    CaseInsensitiveWordTokenizer(),
                    { CharIndex() },    // TODO looks dirty, is there better way like C# 'new generic type constraint' or Rust "static" trait members?
                    this
                ).use { docColl ->

//                val x = readln()
//
//                docColl.waitForIndexFinish()
//                println("wait finished")

                    var q = readln()
                    while (true) {
                        when {
                            q.startsWith(EXIT_COMMAND) -> break
                            q.startsWith(QUERY_COMMAND) -> query(docColl, q.replace(QUERY_COMMAND, ""))
                            q.startsWith(REGISTER_COMMAND) -> {
                                val path = q.replace(REGISTER_COMMAND, "")
                                when (val res = docColl.registerDir(Path(path))) {
                                    RegisterDirResult.Ok -> println("$path being watched now")
                                    RegisterDirResult.AlreadyWatched -> println("$path is already watched")
                                    is RegisterDirResult.Error -> println("$path watch encountered an error: ${res.ex}")
                                }
                            }

                            q.startsWith(UNREGISTER_COMMAND) -> {
                                val path = q.replace(UNREGISTER_COMMAND, "")
                                when (val res = docColl.unregisterDir(Path(path))) {
                                    is UnegisterDirResult.Error -> println("$path unwatch encountered an error: ${res.ex}")
                                    UnegisterDirResult.Ok -> println("$path is not watched now")
                                    UnegisterDirResult.ParentWatched -> println("$path's parent is watched (so it would be watched)")
                                    UnegisterDirResult.WasNotWatched -> println("$path was not watched already")
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