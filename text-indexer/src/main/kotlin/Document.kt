import kotlinx.coroutines.*
import java.io.File
import java.lang.Exception

class Document(val file: File) {
    private var deferredIndex: CompletableDeferred<Index<CharIndex.LinePos>> = CompletableDeferred()
    private var indexBuildJob: Job? = null
    private val scope = CoroutineScope(Dispatchers.IO)

    init {
        scope.launch { buildIndex() }
    }

    fun buildIndex() {
        indexBuildJob = scope.launch {
            val index = CharIndex()
            val tokenizer = Tokenizer()

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

    private suspend fun getIndex(): Index<CharIndex.LinePos> {
        while (true) {
            try {
                val r = deferredIndex.await()
                println("index ready")
                return r
            } catch (e: Exception) {
                println("index ex $e")
            }
        }
    }

    suspend fun queryString(s: String): ArrayList<CharIndex.LinePos> {
        return getIndex().getPositions(s)
    }
}