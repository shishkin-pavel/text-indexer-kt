import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.File
import java.lang.Exception

class Document<TPos>(
    val file: File,
    private var tokenizer: Tokenizer<TPos>,
    val emptyIndex: () -> Index<TPos>,
    private val scope: CoroutineScope
) :
    AutoCloseable {
    @Volatile
    private var deferredIndex: CompletableDeferred<Index<TPos>> = CompletableDeferred()

    @Volatile
    private var indexBuildJob: Job? = null

    @Volatile
    private var isClosed = false

    private val resultSettingMonitor = Mutex()

    init {
        scope.launch { buildIndex() }
    }

    private fun buildIndex() {
        indexBuildJob = scope.launch {
            val index: Index<TPos> = emptyIndex()
            val tch = tokenizer.tokenize(file, this)
            for ((token, pos) in tch) {
                index.addToken(token, pos)
            }
            resultSettingMonitor.withLock {
                deferredIndex.complete(index)
                indexBuildJob = null
            }
        }
    }

    suspend fun rebuildIndex() {
        resultSettingMonitor.withLock {
            indexBuildJob?.cancel()
            if (!deferredIndex.isActive) {
                val old = deferredIndex
                deferredIndex = CompletableDeferred()
                old.cancel()
            }
        }

        buildIndex()
    }

    suspend fun getIndex(): Index<TPos> {
        while (!isClosed) {
            try {
                return deferredIndex.await()
            } catch (_: Exception) {
            }
        }
        throw Exception("document was disposed")    // TODO closing queried document can introduce several problems, lets deal with that later
    }

    suspend fun queryString(s: String): ArrayList<TPos> {
        return getIndex().getPositions(tokenizer.sanitizeToken(s))
    }

    override fun close() {
        if (isClosed) {
            return
        }
        isClosed = true
        indexBuildJob?.cancel("document was disposed")
        if (deferredIndex.isActive) {
            deferredIndex.cancel("document was disposed")
        }
    }
}