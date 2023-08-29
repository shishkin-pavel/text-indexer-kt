import kotlinx.coroutines.*
import org.mozilla.universalchardet.UniversalDetector
import java.io.File
import java.io.FileInputStream
import java.lang.Exception
import java.nio.charset.Charset
import kotlin.coroutines.coroutineContext

class Document(val file: File) {
    private var index: Index<CharIndex.LinePos>? = null

    private suspend fun buildIndex(): Index<CharIndex.LinePos> {
        val index = CharIndex()
        val tokenizer = Tokenizer()

        val scope = CoroutineScope(Dispatchers.IO)
        val tch = tokenizer.tokenize(file, scope)
        for ((token, pos) in tch) {
            index.addToken(token, pos)
        }
        return index
    }

    private suspend fun getIndex(): Index<CharIndex.LinePos> {
        if (index == null) {
            index = buildIndex()
        }
        return index!!
    }

    suspend fun queryString(s: String): ArrayList<CharIndex.LinePos> {
        return getIndex().getPositions(s)
    }
}