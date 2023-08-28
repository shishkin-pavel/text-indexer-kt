import java.util.*
import kotlin.collections.ArrayList

interface Index<TPos> {

    fun addToken(token: ByteArray, pos: TPos)
    fun getPositions(token: ByteArray): ArrayList<TPos>
}

class Trie<TPos> {
    private val root = Node<TPos>()

    private class Node<TPos>(
        val children: TreeMap<Byte, Node<TPos>> = TreeMap(),
        val positions: ArrayList<TPos> = ArrayList()
    )

    fun insert(token: ByteArray, pos: TPos) {
        var curr = root

        for (i in token.indices) {
            val c = token[i]
            val nextNode = curr.children[c];
            if (nextNode == null) {
                var last = Node<TPos>()
                last.positions += pos
                for (j in (token.size - 1) downTo i) {
                    val new = Node<TPos>()
                    new.children[token[j]] = last
                    last = new
                }
                curr.children[c] = last
                return
            }

            curr = nextNode;
        }

        curr.positions += pos
    }

    fun getPositions(token: ByteArray): ArrayList<TPos> {
        var curr = root
        for (c in token) {
            val next = curr.children[c]
            if (next == null) {
                return arrayListOf()
            } else {
                curr = next
            }
        }
        return curr.positions
    }
}

class ByteIndex : Index<ByteIndex.BytePos> {
    data class BytePos(val byteStart: UInt, val byteLen: UInt)

    private val trie = Trie<BytePos>()

    override fun addToken(token: ByteArray, pos: BytePos) {
        trie.insert(token, pos)
    }

    override fun getPositions(token: ByteArray): ArrayList<BytePos> {
        return trie.getPositions(token)
    }
}

class CharIndex : Index<CharIndex.LinePos> {
    data class LinePos(val line: UInt, val shift: UInt)

    private val trie = Trie<LinePos>()

    override fun addToken(token: ByteArray, pos: LinePos) {
        trie.insert(token, pos)
    }

    override fun getPositions(token: ByteArray): ArrayList<LinePos> {
        return trie.getPositions(token)
    }
}