import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import java.nio.file.Path
import kotlinx.coroutines.*
import java.io.File
import java.util.concurrent.ConcurrentHashMap

@OptIn(DelicateCoroutinesApi::class)
class DocumentCollection<TPos>(
    rootPath: Path,
    private val tokenizer: Tokenizer<TPos>,
    private val emptyIndex: () -> Index<TPos>,
    private val scope: CoroutineScope
) : AutoCloseable {
    private val documents = ConcurrentHashMap<String, Document<TPos>>()

    private val fileWatcherContext = newSingleThreadContext("file watcher thread")
    private val fileIndexerContext =
        newSingleThreadContext("file indexer thread") //todo eliminate second single-thread context?
    private val fileWatcher = FileWatcher()

    private val fileWatcherJob: Job

    init {
        fileWatcherJob = scope.launch {
            coroutineScope {
                launch(fileWatcherContext) {
                    fileWatcher.watchDirectoryTree(rootPath)
                }
                launch(fileIndexerContext) {
                    watchChanges(fileWatcher.eventChannel)
                }
            }
        }
    }

    private suspend fun watchChanges(fileWatchEvents: Channel<FileWatchEvent>) {
        for (fwe in fileWatchEvents) {
            when (fwe.kind) {
                FileWatchEvent.Kind.Created -> {
                    val doc = Document(File(fwe.path), tokenizer, emptyIndex, scope)
                    documents[fwe.path] = doc
                }

                FileWatchEvent.Kind.Modified -> {
                    val doc = documents[fwe.path]!!
                    doc.rebuildIndex()
                }

                FileWatchEvent.Kind.Deleted -> {
                    val doc = documents.remove(fwe.path)!!
                    doc.close()
                }
            }
        }
    }

    suspend fun query(str: String): List<Pair<Path, ArrayList<TPos>>> {
        return coroutineScope {
            async(Dispatchers.Default) {
                documents.values.map {
                    async {
                        Pair(it.file.toPath(), it.queryString(str))
                    }
                }.map {
                    it.await()
                }
            }
        }.await()
    }

    suspend fun waitForIndexFinish() {
        println("waiting for ${documents.size} indexes")
        val indexes = coroutineScope {
            documents.values.map {
                async { it.getIndex() }
            }.map { it.await() }
        }
    }

    override fun close() {
        fileWatcherJob.cancel()
        for (d in documents.values) {
            d.close()
        }
        println("cancel requested")
    }
}