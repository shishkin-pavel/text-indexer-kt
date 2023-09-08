import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.mozilla.universalchardet.UniversalDetector
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.charset.Charset
import java.nio.file.StandardOpenOption

interface Tokenizer<TPos> {
    fun sanitizeToken(str: String): String

    fun tokenize(
        file: File,
        scope: CoroutineScope
    ): Channel<Pair<String, TPos>>
}

suspend fun <T> retry(
    times: Int = Int.MAX_VALUE,
    initialDelay: Long = 1000,
    maxDelay: Long = 30000,
    factor: Double = 2.0,
    block: suspend () -> T
): Result<T> {
    var currentDelay = initialDelay
    var lastEx: Exception? = null
    repeat(times) {
        try {
            return@retry Result.success(block())
        } catch (e: Exception) {
            println("retrying: ${e.message}")
            lastEx = e
        }

        delay(currentDelay)
        currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelay)
    }
    return Result.failure(lastEx!!)
}

class CaseInsensitiveWordTokenizer(private val defaultEncoding: Charset = Charsets.UTF_8) :
    Tokenizer<CharIndex.LinePos> {
    private val punctuationInWord = setOf('\'', '-', '_')

    private fun validWordChar(c: Char): Boolean {
        return c.isLetterOrDigit() || punctuationInWord.contains(c)
    }

    override fun sanitizeToken(str: String): String {
        return str.lowercase()
    }

    private fun detectCharset(file: File): Charset {
        val detector = UniversalDetector(null)
        val buf = ByteBuffer.allocate(4096)
        val encoding =
            AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ).use { fis ->
                var totalRead = 0L
                var nread: Int
                while (fis.read(buf, totalRead).get()
                        .also { nread = it; totalRead += it } > 0
                    && !detector.isDone // would be great if we could set that check to be the first, but compiler blames me that `nread` is uninitialized in that case (analysys bug?)
                ) {
                    detector.handleData(buf.array(), 0, nread)
                }

                detector.dataEnd()
                val detectedEncoding = detector.detectedCharset
                detector.reset()
                detectedEncoding
            }

        if (encoding != null) {
            return Charset.availableCharsets()[encoding] ?: defaultEncoding
        }
        return defaultEncoding
    }

    override fun tokenize(
        file: File,
        scope: CoroutineScope
    ): Channel<Pair<String, CharIndex.LinePos>> {
        val ch = Channel<Pair<String, CharIndex.LinePos>>(10)

        scope.launch {
            withContext(Dispatchers.IO) {
                retry {
                    val charset = detectCharset(file)
                    file.useLines(charset) { lines ->
                        lines.forEachIndexed { lineNum, line ->
                            val tokenBoundaries = ArrayList<Pair<Int, Int>>()
                            var tokenStart: Int? = null
                            line.forEachIndexed { idx, c ->
                                if (validWordChar(c)) {
                                    if (tokenStart == null) {
                                        tokenStart = idx
                                    }
                                } else {
                                    if (tokenStart != null) {
                                        tokenBoundaries += Pair(tokenStart!!, (idx - 1))
                                        tokenStart = null
                                    }
                                }
                            }
                            if (tokenStart != null) {
                                tokenBoundaries += Pair(tokenStart!!, (line.length - 1))
                            }

                            for ((s, e) in tokenBoundaries) {
                                val str = sanitizeToken(line.substring(s..e))
                                val p = Pair(str, CharIndex.LinePos(lineNum + 1, s))
                                ch.send(p)
                            }
                        }
                    }
                    ch.close()
                }
            }
        }
        return ch
    }
}