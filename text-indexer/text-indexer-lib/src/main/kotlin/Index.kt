import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.*
import kotlin.collections.ArrayList
import kotlin.math.abs

interface Index<TPos> {

    fun addToken(token: String, pos: TPos)
    fun getPositions(token: String): ArrayList<TPos>
}

class Trie<TPos> {
    private val root = Node<TPos>()

    private class Node<TPos>(
        val children: TreeMap<Char, Node<TPos>> = TreeMap(),
        val positions: ArrayList<TPos> = ArrayList()
    )

    fun insert(token: String, pos: TPos) {
        var curr = root

        for (i in token.indices) {
            val c = token[i]
            val nextNode = curr.children[c]
            if (nextNode == null) {
                var last = Node<TPos>()
                last.positions += pos
                for (j in (token.length - 1) downTo (i + 1)) {
                    val new = Node<TPos>()
                    new.children[token[j]] = last
                    last = new
                }
                curr.children[c] = last
                return
            }

            curr = nextNode
        }

        curr.positions += pos
    }

    fun getPositions(token: String): ArrayList<TPos> {
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

class BloomFilter(private val bitCount: Int, private val hashFunctionsNum: Int) {
    private val bitSet = BitSet(bitCount)
    private val md = MessageDigest.getInstance("MD5")
    private val salt = ByteArray(hashFunctionsNum)

    init {
        // different salt for every instance of bloom filter will make hash attacks close to impossible
        // we can enlarge size of salt to several bytes to make it less predictable
        Random().nextBytes(salt)
    }

    private fun getHashValues(item: String): List<Int> {
        val hashValues = mutableListOf<Int>()

        for (i in 0 until hashFunctionsNum) {
            md.update(salt[i])
            val digest = md.digest(item.toByteArray())
            val buffer = ByteBuffer.wrap(digest)
            hashValues.add(buffer.int)
        }

        return hashValues
    }

    fun add(item: String) {
        val hashValues = getHashValues(item)
        for (hash in hashValues) {
            bitSet.set(abs(hash) % bitCount)
        }
    }

    fun mightContain(item: String): Boolean {
        val hashValues = getHashValues(item)
        for (hash in hashValues) {
            if (!bitSet.get(abs(hash) % bitCount)) {
                return false
            }
        }
        return true
    }
}

// can be useful for search & replace purposes
class ByteIndex : Index<ByteIndex.BytePos> {
    data class BytePos(val byteStart: UInt, val byteLen: UInt) {
        override fun toString(): String {
            return "0x${byteStart.toString(16)}[0x${byteLen.toString(16)}]"
        }
    }

    private val trie = Trie<BytePos>()

    override fun addToken(token: String, pos: BytePos) {
        trie.insert(token, pos)
    }

    override fun getPositions(token: String): ArrayList<BytePos> {
        return trie.getPositions(token)
    }
}

class CharIndex : Index<CharIndex.LinePos> {
    data class LinePos(val line: Int, val shift: Int) {
        override fun toString(): String {
            return "$line:$shift"
        }
    }

    private val trie = Trie<LinePos>()
    // probably we can play with bitcount/hashfunctions number, but manual fitting showed worse results than without filters
//    private val bloomFilter = BloomFilter(10000, 3)

    override fun addToken(token: String, pos: LinePos) {
//        bloomFilter.add(token)
        trie.insert(token, pos)
    }

    override fun getPositions(token: String): ArrayList<LinePos> {
//        if (bloomFilter.mightContain(token)) {
        return trie.getPositions(token)
//        } else {
//            return arrayListOf()
//        }
    }
}