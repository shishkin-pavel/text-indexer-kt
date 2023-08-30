import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.mozilla.universalchardet.UniversalDetector
import java.io.File
import java.io.FileInputStream
import java.nio.charset.Charset

class Tokenizer {
    private val punctuationInWord = setOf('\'', '-', '_')
    private val detector = UniversalDetector(null)

    private fun validWordChar(c: Char): Boolean {
        return c.isLetterOrDigit() || punctuationInWord.contains(c)
    }

    private fun detectCharset(file: File): Charset {
        val buf = ByteArray(4096)
        val fis = FileInputStream(file)

        var totalRead = 0
        var nread: Int
        while (fis.read(buf).also { nread = it; totalRead += it } > 0 && !detector.isDone) {
            detector.handleData(buf, 0, nread)
        }

        detector.dataEnd()
        val encoding = detector.detectedCharset
        if (encoding == null) {
//            println("encoding was not detected, UTF-8 will be used")
            return Charsets.UTF_8
        }
        detector.reset()

        fis.close()

        val charset = Charset.availableCharsets()[encoding]

        if (charset == null) {
//            println("no suitable charset supported, UTF-8 will be used")
        } else {
//            println("charset: $encoding, detected after $totalRead bytes")
        }

        return charset ?: Charsets.UTF_8
    }

    fun tokenize(
        file: File,
        scope: CoroutineScope
    ): Channel<Pair<String, CharIndex.LinePos>> {
        val ch = Channel<Pair<String, CharIndex.LinePos>>(10)

        scope.launch {
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
                        val substr = line.substring(s..e)
                        val p = Pair(substr, CharIndex.LinePos(lineNum + 1, s))
                        ch.send(p)
                    }
                }
            }
            ch.close()
        }
        return ch
    }
}