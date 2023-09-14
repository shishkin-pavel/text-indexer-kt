import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.mozilla.universalchardet.UniversalDetector
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.charset.Charset
import java.nio.file.StandardOpenOption
import kotlin.system.measureTimeMillis

const val BUF_SIZE = 4096
const val MAX_BYTES = 20 * BUF_SIZE

interface Tokenizer<TPos> {
    fun sanitizeToken(str: String): String

    fun tokenize(file: File, scope: CoroutineScope): Channel<Pair<String, TPos>>

    // rewrite without use of channels
    suspend fun tokens(file: File, scope: CoroutineScope): MutableMap<String, MutableList<TPos>> {
        val m = mutableMapOf<String, MutableList<TPos>>()

        val ch = tokenize(file, scope)
        for ((token, pos) in ch) {
            m.compute(token) { _, positions ->
                if (positions == null) {
                    mutableListOf(pos)
                } else {
                    positions.add(pos)
                    positions
                }
            }
        }

        return m
    }
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
            lastEx = e
        }

        delay(currentDelay)
        currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelay)
    }
    return Result.failure(lastEx!!)
}

data class LinePos(val line: Int, val shift: Int) {
    override fun toString(): String {
        return "$line:$shift"
    }
}

class CaseInsensitiveWordTokenizer(private val defaultEncoding: Charset = Charsets.UTF_8) :
    Tokenizer<LinePos> {
    private val punctuationInWord = setOf('\'', '-', '_')

    private fun validWordChar(c: Char): Boolean {
        return c.isLetterOrDigit() || punctuationInWord.contains(c)
    }

    override fun sanitizeToken(str: String): String {
        return str.lowercase()
    }

    private fun detectCharset(file: File): Result<Charset> {
        val detector = UniversalDetector(null)
        val buf = ByteBuffer.allocate(BUF_SIZE)
        val encoding =
            AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ).use { fis ->
                var totalRead = 0L
                var nread: Int
                while (fis.read(buf, totalRead).get()
                        .also { nread = it; totalRead += it } > 0
                    && !detector.isDone // would be great if we could set that check to be the first, but compiler blames me that `nread` is uninitialized in that case (analysys bug?)
                    && totalRead < MAX_BYTES
                ) {
                    detector.handleData(buf.array(), 0, nread)
                }

                detector.dataEnd()
                val detectedEncoding = detector.detectedCharset
                detector.reset()
                detectedEncoding
            }

        if (encoding != null) {
            return Result.success(Charset.availableCharsets()[encoding] ?: defaultEncoding)
        }
        return Result.failure(Exception("seems like a non-text file"))
    }

    override fun tokenize(
        file: File,
        scope: CoroutineScope
    ): Channel<Pair<String, LinePos>> {
        val ch = Channel<Pair<String, LinePos>>(100)

        scope.launch {
            withContext(Dispatchers.IO) {
                retry {
//                    logger.info { "tokenization start for ${file.toPath()}" }
                    var tokenCount = 0
                    val tokenizationTime = measureTimeMillis {
                        val charsetRes = detectCharset(file)
                        charsetRes.onSuccess { charset ->
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
                                        val p = Pair(str, LinePos(lineNum + 1, s))
                                        ch.send(p)
                                        tokenCount++
                                    }
                                }
                            }
                        }
                        ch.close()
                    }
//                    logger.info { ("tokenization for ${file.toPath()} finished in $tokenizationTime ms, got $tokenCount tokens") }
                }
            }
        }
        return ch
    }
}