import java.nio.file.Path

class Trie<TPos> {
    private val root = Node<TPos>()

    private class Node<TPos>(
        val children: MutableMap<Char, Node<TPos>> = mutableMapOf(),
        val positions: MutableMap<Path, MutableList<TPos>> = mutableMapOf()
    )

    fun insert(token: String, doc: Path, positions: List<TPos>) {
        var curr = root

        for (i in token.indices) {
            val c = token[i]
            val nextNode = curr.children[c]
            if (nextNode == null) {
                var last = Node<TPos>()
                last.positions.getOrPut(doc) { mutableListOf() } += positions
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

        curr.positions.getOrPut(doc) { mutableListOf() } += positions
    }

    fun getPositions(token: String): Map<Path, List<TPos>> {
        var curr = root
        for (c in token) {
            val next = curr.children[c]
            if (next == null) {
                return mapOf()
            } else {
                curr = next
            }
        }
        return curr.positions
    }
}