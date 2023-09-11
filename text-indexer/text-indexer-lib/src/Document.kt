import kotlinx.coroutines.*
import java.io.File
import java.lang.Exception

class Document<TPos>(val file: File, private var tokenizer: Tokenizer<TPos>, val emptyIndex: () -> Index<TPos>) :
    AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.IO)
    private var deferredIndex: CompletableDeferred<Index<TPos>> = CompletableDeferred()
    private var indexBuildJob: Job? = null
    private var isClosed = false

    init {
        scope.launch { buildIndex() }
    }

    private fun buildIndex() {
        indexBuildJob = scope.launch {
            val index: Index<TPos> = emptyIndex()
            val scope = CoroutineScope(Dispatchers.IO)
            val tch = tokenizer.tokenize(file, scope)
            for ((token, pos) in tch) {
                index.addToken(token, pos)
            }
            deferredIndex.complete(index)
            indexBuildJob = null
        }
    }

    fun rebuildIndex() {
        indexBuildJob?.cancel()
        if (!deferredIndex.isActive) {
            val old = deferredIndex
            deferredIndex = CompletableDeferred()
            old.cancel()
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
        isClosed = true
        scope.cancel("document was disposed")
        if (deferredIndex.isActive) {
            deferredIndex.cancel("document was disposed")
        }
    }
}