@file:OptIn(ExperimentalCoroutinesApi::class)

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.nio.file.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

class SingleIndexDocumentCollection<TPos>(private val tokenizer: Tokenizer<TPos>, private val scope: CoroutineScope) :
    AutoCloseable {

    suspend fun addWatch(path: Path): Pair<Deferred<RegisterDirResult>, Deferred<Unit>> {
        val resDeferred = CompletableDeferred<RegisterDirResult>()
        val indexDeferred = CompletableDeferred<Unit>()
        coordinationCh.send(CoordinatorMessage.AddWatch(path, resDeferred, indexDeferred))
        return Pair(resDeferred, indexDeferred)
    }

    suspend fun removeWatch(path: Path): Pair<Deferred<UnregisterDirResult>, Deferred<Unit>> {
        val resDeferred = CompletableDeferred<UnregisterDirResult>()
        val indexDeferred = CompletableDeferred<Unit>()
        coordinationCh.send(CoordinatorMessage.RemoveWatch(path, resDeferred, indexDeferred))
        return Pair(resDeferred, indexDeferred)
    }

    suspend fun query(q: String): Map<Path, List<TPos>> {
        val deferred = CompletableDeferred<Map<Path, List<TPos>>>()
        coordinationCh.send(CoordinatorMessage.Query(q, deferred))
        return deferred.await()
    }

    private sealed class CoordinatorMessage {
        data object WorkerStart : CoordinatorMessage()
        data object WorkerEnd : CoordinatorMessage()
        data class Query<TPos>(val q: String, val resultReceiver: CompletableDeferred<Map<Path, List<TPos>>>) :
            CoordinatorMessage()

        data class AddWatch(
            val path: Path,
            val resultReceiver: CompletableDeferred<RegisterDirResult>,
            val indexFinished: CompletableDeferred<Unit>
        ) :
            CoordinatorMessage()

        data class RemoveWatch(
            val path: Path,
            val resultReceiver: CompletableDeferred<UnregisterDirResult>,
            val indexFinished: CompletableDeferred<Unit>
        ) :
            CoordinatorMessage()
    }

    private data class IndexMutation<TPos>(
        val doc: Path,
        val newTokens: Map<String, List<TPos>>,
        val processingFinish: CompletableDeferred<Unit>
    )

    private val coordinationCh = Channel<CoordinatorMessage>()
    private val indexes = ConcurrentHashMap<Char, Pair<Channel<IndexMutation<TPos>>, Trie<TPos>>>()
    private val coordinatorJob: Job
    private val fileJobs = ConcurrentHashMap<Path, Job>()
    private val shardJobs = ConcurrentLinkedQueue<Job>()
    private var pendingQueries = mutableListOf<CoordinatorMessage.Query<TPos>>()
    private var pendingIndexFinish = mutableListOf<CompletableDeferred<Unit>>()

    // dedicated thread for filesystem events as soon as we block on querying events
    private val fileWatcher = FileWatcher(
        { path -> indexDocument(path, scope) },
        { path -> indexDocument(path, scope) },
        { path -> deleteDocument(path, scope) },
        scope
    )

    init {
        var busyWorkers = 0

        coordinatorJob = scope.launch(CoroutineName("coordinator")) {
            while (isActive) {
                if (coordinationCh.isEmpty && busyWorkers == 0) {
                    val pending = pendingIndexFinish
                    pendingIndexFinish = mutableListOf()
                    for (p in pending) {
                        p.complete(Unit)
                    }
                    val queries = pendingQueries
                    pendingQueries = mutableListOf()
                    queries.map { async { queryImpl(it) } }.map { it.await() }
                }
                when (val command = coordinationCh.receive()) {
                    CoordinatorMessage.WorkerStart -> busyWorkers += 1
                    CoordinatorMessage.WorkerEnd -> busyWorkers -= 1

                    is CoordinatorMessage.Query<*> -> {
                        @Suppress("UNCHECKED_CAST") // it is always safe because only commands produced by that class can end up here
                        val query = command as CoordinatorMessage.Query<TPos>
                        pendingQueries.add(query)
                    }

                    is CoordinatorMessage.AddWatch -> {
                        val res = fileWatcher.registerDirTree(command.path)
                        command.resultReceiver.complete(res)
                        pendingIndexFinish += command.indexFinished
                    }

                    is CoordinatorMessage.RemoveWatch -> {
                        val res = fileWatcher.unregisterDirTree(command.path)
                        command.resultReceiver.complete(res)
                        pendingIndexFinish += command.indexFinished
                    }
                }
            }
        }
    }

    private suspend fun <T> startWorker(block: suspend () -> T): T {
        try {
            coordinationCh.send(CoordinatorMessage.WorkerStart)
            return block()
        } finally {
            coordinationCh.send(CoordinatorMessage.WorkerEnd)
        }
    }

    private fun spawnShard(char: Char): Pair<Channel<IndexMutation<TPos>>, Trie<TPos>> {
        val ch = Channel<IndexMutation<TPos>>()
        val trie = Trie<TPos>()
        val job = scope.launch(CoroutineName("'$char' shard")) {
            // TODO remove old doc tokens first
            for (p in ch) {
                for ((token, pos) in p.newTokens) {
                    trie.insert(token, p.doc, pos)
                }
                p.processingFinish.complete(Unit)
            }
        }
        shardJobs.add(job)
        return Pair(ch, trie)
    }

    private fun getOrSpawnChannel(char: Char): Channel<IndexMutation<TPos>> {
        val (ch, _) = indexes.getOrPut(char) { spawnShard(char) }
        return ch
    }

    private fun getTrie(char: Char): Trie<TPos>? {
        val chTrie = indexes.getOrPut(char) { spawnShard(char) }
        return chTrie?.second
    }

    private fun indexDocument(path: Path, scope: CoroutineScope) {
        val startedJob = scope.launch(CoroutineName("file job $path")) {
            val thatJob = coroutineContext[Job.Key]
            coroutineScope {
                startWorker {
                    val allTokens = tokenizer.tokens(path.toFile(), this)
                    allTokens.remove("")

                    val byFirstChar = allTokens.keys.groupBy { it[0] }

                    val notPresentShards = indexes.keys.toMutableSet()
                    notPresentShards.removeAll(byFirstChar.keys)

                    val deleteAndInsert = byFirstChar.map { (firstChar, tokenList) ->
                        val tokens2positions = tokenList.associateWith { allTokens[it]!! }
                        val ch = getOrSpawnChannel(firstChar)
                        val completed = CompletableDeferred<Unit>()
                        ch.send(IndexMutation(path, tokens2positions, completed))
                        completed
                    }
                    val delete = notPresentShards.map { c ->
                        val ch = getOrSpawnChannel(c)
                        val completed = CompletableDeferred<Unit>()
                        ch.send(IndexMutation(path, emptyMap(), completed))
                        completed
                    }
                    deleteAndInsert.forEach { it.await() }
                    delete.forEach { it.await() }
                }
                fileJobs.compute(path) { _, job ->
                    if (job == thatJob) {
                        job?.cancel()
                        null
                    } else {
                        job
                    }
                }
            }
        }
        fileJobs.compute(path) { _, job ->
            job?.cancel()
            startedJob
        }
    }

    private fun deleteDocument(path: Path, scope: CoroutineScope) {
        val startedJob = scope.launch(CoroutineName("file delete $path")) {
            coroutineScope {
                startWorker {
                    val notPresentShards = indexes.keys.toSet()
                    notPresentShards.map { c ->
                        val ch = getOrSpawnChannel(c)
                        val completed = CompletableDeferred<Unit>()
                        ch.send(IndexMutation(path, emptyMap(), completed))
                        completed
                    }.forEach { it.await() }
                }
                fileJobs.compute(path) { _, job ->
                    job?.cancel()
                    null
                }
            }
        }

        fileJobs.compute(path) { _, job ->
            job?.cancel()
            startedJob
        }
    }

    private fun queryImpl(queryCommand: CoordinatorMessage.Query<TPos>) {
        val str = queryCommand.q
        if (str.isEmpty()) {
            queryCommand.resultReceiver.cancel()
            return
        }
        val trie = getTrie(str[0])
        if (trie == null) {
            queryCommand.resultReceiver.complete(mapOf())
        } else {
            val res = trie.getPositions(str)
            queryCommand.resultReceiver.complete(res)
        }
    }

    override fun close() {
        println("cancel requested")
        fileWatcher.close()
        println("fileJobs: ${fileJobs.size}")
        for (fj in fileJobs.values.toList()) {
            fj.cancel()
        }

        for (j in pendingQueries) {
            j.resultReceiver.cancel()
        }
        for (j in pendingIndexFinish) {
            println("cancelling indexing")
            j.cancel()
        }

        coordinatorJob.cancel()
        for ((ch, _) in indexes.values) {
            ch.close()
        }
        for (shardJob in shardJobs.toList()) {
            shardJob.cancel()
        }
        println("scope: $scope")
        val childCoroutines = bfs(scope.coroutineContext.job, { it.children.toList() }, { it }).map { it.first }
        println(childCoroutines.joinToString("\n") { "${(it as CoroutineScope).coroutineContext}" })
    }
}