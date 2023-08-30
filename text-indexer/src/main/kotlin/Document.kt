import kotlinx.coroutines.*
import java.io.File
import java.lang.Exception

class Document(val file: File) : AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.IO)
    private var deferredIndex: CompletableDeferred<Index<CharIndex.LinePos>> = CompletableDeferred()
    private var indexBuildJob: Job? = null
    private var isClosed = false

    init {
        scope.launch { buildIndex() }
    }

    private fun buildIndex() {
        indexBuildJob = scope.launch {
            val index = CharIndex()
            val tokenizer = Tokenizer()

            val scope = CoroutineScope(Dispatchers.IO)
            val tch = tokenizer.tokenize(file, scope)
            for ((token, pos) in tch) {
                index.addToken(token, pos)
            }
            println("${file.path} analysis completed")
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
        while (!isClosed) {
            try {
                val r = deferredIndex.await()
                println("index ready")
                return r
            } catch (e: Exception) {
                println("index ex $e")
            }
        }
        throw Exception("document was disposed")    // TODO closing queried document can introduce several problems, lets deal with that later
    }

    suspend fun queryString(s: String): ArrayList<CharIndex.LinePos> {
        return getIndex().getPositions(s)
    }

    override fun close() {
        isClosed = true
        scope.cancel("document was disposed")
        if (deferredIndex.isActive) {
            deferredIndex.cancel("document was disposed")
        }
    }
}