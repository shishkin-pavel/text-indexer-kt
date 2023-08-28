import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.io.File

class Tokenizer {
    private val punctuationInWord = setOf('\'', '-', '_')

    private fun validWordChar(c: Char): Boolean {
        return c.isLetterOrDigit() || punctuationInWord.contains(c)
    }

    fun tokenize(file: File): Channel<Pair<ByteArray, CharIndex.LinePos>> {
        val ch = Channel<Pair<ByteArray, CharIndex.LinePos>>()

        GlobalScope.launch {
            file.useLines { lines ->
                lines.forEachIndexed { lineNum, line ->
                    var tokenBoundaries = ArrayList<Pair<Int, Int>>()
                    var tokenStart: Int? = null
                    line.forEachIndexed { idx, c ->
                        if (validWordChar(c)) {
                            if (tokenStart == null) {
                                tokenStart = idx
                            }
                        } else {
                            if (tokenStart != null) {
                                tokenBoundaries += Pair(tokenStart!!, (idx - 1))
                            }
                        }
                    }
                    if (tokenStart != null) {
                        tokenBoundaries += Pair(tokenStart!!, (line.length - 1))
                    }

                    for ((s, e) in tokenBoundaries) {
                        val substr = line.substring(s..e)
                        val p = Pair(substr.toByteArray(), CharIndex.LinePos(lineNum.toUInt(), 0u))
                        ch.send(p)
                    }
                }
            }
            ch.close()
        }



        return ch
    }
}