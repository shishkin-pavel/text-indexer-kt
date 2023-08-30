import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import java.nio.file.Path
import kotlinx.coroutines.*

class DocumentCollection {
    private val documents = HashMap<Path, Document>()

    suspend fun watchChanges(fileWatchEvents: Channel<FileWatchEvent>) {
        for (fwe in fileWatchEvents) {
            when (fwe.kind) {
                FileWatchEvent.Kind.Created -> {
                    val doc = Document(fwe.path.toFile())
                    documents[fwe.path] = doc
                }

                FileWatchEvent.Kind.Modified -> {
                    val doc = documents[fwe.path]!!
                    doc.rebuildIndex()
                }

                FileWatchEvent.Kind.Deleted -> {
                    documents.remove(fwe.path)
                }
            }
        }
    }

    suspend fun query(str: String): List<Pair<Path, ArrayList<CharIndex.LinePos>>> {
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