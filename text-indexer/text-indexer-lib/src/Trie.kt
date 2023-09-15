import java.nio.file.Path

class Trie<TPos> {
    private val root = Node<TPos>()
    private val documentTokens = mutableMapOf<Path, Set<String>>()

    private class Node<TPos>(
        val children: MutableMap<Char, Node<TPos>> = mutableMapOf(),
        val positions: MutableMap<Path, List<TPos>> = mutableMapOf()
    )

    private fun store(path: Path, token: String, positions: List<TPos>) {
        var curr = root

        for (i in token.indices) {
            val c = token[i]
            val nextNode = curr.children[c]
            if (nextNode == null) {
                var last = Node<TPos>()
                last.positions[path] = positions
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

        curr.positions[path] = positions
    }

    private fun removeSingle(path: Path, token: String) {
        var curr = root

        for (c in token) {
            val next = curr.children[c]
            if (next == null) {
                return
            } else {
                curr = next
            }
        }
        curr.positions.remove(path)
    }

    private fun remove(path: Path, tokens: Collection<String>) {
        // tod ogrouped removal
        for (t in tokens) {
            removeSingle(path, t)
        }
    }

    fun set(path: Path, newTokens: Map<String, List<TPos>>) {
        val oldTokens = documentTokens[path]
        if (oldTokens != null) {
            val difference = oldTokens - newTokens.keys
            remove(path, difference)
        }

        documentTokens[path] = newTokens.keys
        for ((token, positions) in newTokens) {
            store(path, token, positions)
        }
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