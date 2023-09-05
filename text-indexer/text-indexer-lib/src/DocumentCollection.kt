import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import java.nio.file.Path
import kotlinx.coroutines.*
import java.util.concurrent.ConcurrentHashMap

@OptIn(DelicateCoroutinesApi::class)
class DocumentCollection<TPos>(
    rootPath: Path,
    private val tokenizer: Tokenizer<TPos>,
    private val emptyIndex: () -> Index<TPos>,
    scope: CoroutineScope
) {
    private val documents = ConcurrentHashMap<Path, Document<TPos>>()

    private val fileWatcherContext = newSingleThreadContext("file watcher thread")
    private val fileIndexerContext = newSingleThreadContext("file indexer thread")
    private val fileWatcher = FileWatcher()

    init {
        scope.launch(fileWatcherContext) {
            fileWatcher.watchDirectoryTree(rootPath)
        }
        scope.launch(fileIndexerContext) {
            watchChanges(fileWatcher.eventChannel)
        }
    }

    private suspend fun watchChanges(fileWatchEvents: Channel<FileWatchEvent>) {
        for (fwe in fileWatchEvents) {
            when (fwe.kind) {
                FileWatchEvent.Kind.Created -> {
                    val doc = Document(fwe.path.toFile(), tokenizer, emptyIndex)
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
}