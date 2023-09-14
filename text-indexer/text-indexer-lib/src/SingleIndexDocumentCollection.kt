@file:OptIn(ExperimentalCoroutinesApi::class)

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.io.path.absolute
import kotlin.io.path.isDirectory

fun <T, TData, TColl : Collection<T>> bfs(
    start: T,
    getChildren: (T) -> TColl,
    getData: (T) -> TData
): List<Pair<T, TData>> {
    val queue: Queue<T> = LinkedList()
    val result = mutableListOf<Pair<T, TData>>()
    val visited = mutableSetOf<T>()

    queue.add(start)
    visited.add(start)

    while (queue.isNotEmpty()) {
        val current = queue.poll()
        result.add(Pair(current, getData(current)))

        for (c in getChildren(current)) {
            if (!visited.contains(c)) {
                queue.add(c)
                visited.add(c)
            }
        }
    }

    return result
}

sealed class RegisterDirResult {
    data object Ok : RegisterDirResult()
    data object AlreadyWatched : RegisterDirResult()
    data class Error(val ex: Exception) : RegisterDirResult()

}

sealed class UnregisterDirResult {
    data object Ok : UnregisterDirResult()
    data object ParentWatched : UnregisterDirResult()
    data object WasNotWatched : UnregisterDirResult()
    data class Error(val ex: Exception) : UnregisterDirResult()

}

data class DocIndexTask(val path: Path, val cancelled: Boolean) // todo is cancelled needed?

sealed class CoordinatorMessage {
    data object WorkerStart : CoordinatorMessage()
    data object WorkerEnd : CoordinatorMessage()
    data class Query<TPos>(val q: String, val resultReceiver: CompletableDeferred<Map<Path, List<TPos>>>) :
        CoordinatorMessage()

    data class AddWatch(val path: Path, val resultReceiver: CompletableDeferred<RegisterDirResult>) :
        CoordinatorMessage()

    data class RemoveWatch(val path: Path, val resultReceiver: CompletableDeferred<UnregisterDirResult>) :
        CoordinatorMessage()
}

data class IndexMutation<TPos>(
    val doc: Path,
    val newTokens: Map<String, List<TPos>>,
    val processingFinish: CompletableDeferred<Unit>
)

class SingleIndexDocumentCollection<TPos>(private val tokenizer: Tokenizer<TPos>, private val scope: CoroutineScope) :
    AutoCloseable {
    private val coordinationCh = Channel<CoordinatorMessage>()
    private val indexes = ConcurrentHashMap<Char, Pair<Channel<IndexMutation<TPos>>, Trie<TPos>>>()
    private val coordinatorJob: Job

    // dedicated thread for filesystem events as soon as we block on querying events
    private val fileWatcherContext = newSingleThreadContext("file watcher thread")
    private val fileWatcher = FileWatcher()

    private val fileWatcherJob: Job = scope.launch {
        coroutineScope {
            launch(fileWatcherContext) {
                fileWatcher.watch()
            }
        }
    }

    init {
        var busyWorkers = 0
        var pendingQueries = mutableListOf<CoordinatorMessage.Query<TPos>>()

        coordinatorJob = scope.launch {
            while (isActive) {
                if (coordinationCh.isEmpty && busyWorkers == 0) {
                    val queries = pendingQueries
                    pendingQueries = mutableListOf()
                    queries.map { async { queryImpl(it) } }.map { it.await() }
                }
                when (val command = coordinationCh.receive()) {
                    CoordinatorMessage.WorkerStart -> {
                        busyWorkers += 1
                        println("workers: $busyWorkers")
                        if (busyWorkers == 1) {
//                            TODO("mark as indexing stage")
                        }
                    }

                    CoordinatorMessage.WorkerEnd -> {
                        busyWorkers -= 1
                        println("workers: $busyWorkers")
                        if (busyWorkers == 0) {
//                            TODO("mark as index complete")
                        }
                    }

                    is CoordinatorMessage.Query<*> -> {
                        @Suppress("UNCHECKED_CAST") // it is always safe because only commands produced by that class can end up here
                        val query = command as CoordinatorMessage.Query<TPos>
                        if (busyWorkers == 0) {
                            queryImpl(query)
                        } else {
                            pendingQueries.add(query)
                        }
                    }

                    is CoordinatorMessage.AddWatch -> {
                        val res = fileWatcher.registerDirTree(command.path)
                        command.resultReceiver.complete(res)
                    }

                    is CoordinatorMessage.RemoveWatch -> {
                        val res = fileWatcher.unregisterDirTree(command.path)
                        command.resultReceiver.complete(res)
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

    private fun spawnShard(): Pair<Channel<IndexMutation<TPos>>, Trie<TPos>> {
        val ch = Channel<IndexMutation<TPos>>()
        val trie = Trie<TPos>()
        scope.launch {
            // TODO remove old doc tokens first
            for (p in ch) {
                for ((token, pos) in p.newTokens) {
                    trie.insert(token, p.doc, pos)
                }
                p.processingFinish.complete(Unit)
            }
        }
        return Pair(ch, trie)
    }

    private fun getOrSpawnChannel(char: Char): Channel<IndexMutation<TPos>> {
        val (ch, _) = indexes.getOrPut(char) { spawnShard() }
        return ch
    }

    private fun getTrie(char: Char): Trie<TPos>? {
        val chTrie = indexes.getOrPut(char) { spawnShard() }
        return chTrie?.second
    }

    suspend fun indexDocument(path: Path, scope: CoroutineScope) {
        scope.launch {
            coroutineScope { //todo is it needed there?
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
            }
        }
    }

    suspend fun deleteDocument(path: Path, scope: CoroutineScope) {
        scope.launch {
            coroutineScope { //todo is it needed there?
                startWorker {
                    val notPresentShards = indexes.keys.toSet()
                    notPresentShards.map { c ->
                        val ch = getOrSpawnChannel(c)
                        val completed = CompletableDeferred<Unit>()
                        ch.send(IndexMutation(path, emptyMap(), completed))
                        completed
                    }.forEach { it.await() }
                }
            }
        }
    }

    suspend fun addWatch(path: Path): RegisterDirResult {
        val deferred = CompletableDeferred<RegisterDirResult>()
        coordinationCh.send(CoordinatorMessage.AddWatch(path, deferred))
        return deferred.await()
    }

    suspend fun removeWatch(path: Path): UnregisterDirResult {
        val deferred = CompletableDeferred<UnregisterDirResult>()
        coordinationCh.send(CoordinatorMessage.RemoveWatch(path, deferred))
        return deferred.await()
    }

    suspend fun query(q: String): Map<Path, List<TPos>> {
        val deferred = CompletableDeferred<Map<Path, List<TPos>>>()
        coordinationCh.send(CoordinatorMessage.Query(q, deferred))
        return deferred.await()
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
        coordinatorJob.cancel()
        fileWatcherJob.cancel()
        for ((ch, trie) in indexes.values) {
            ch.close()
        }
    }

    private data class DirectoryContent(val dirs: HashSet<Path>, val files: HashSet<Path>) {
        constructor() : this(HashSet(), HashSet())

        fun addDir(absolutePath: Path) {
            dirs.add(absolutePath)
        }

        fun removeDir(absolutePath: Path) {
            dirs.remove(absolutePath)
        }

        fun addFile(absolutePath: Path) {
            files.add(absolutePath)
        }

        fun removeFile(absolutePath: Path): Boolean {
            return files.remove(absolutePath)
        }
    }

    /**
     * Class dedicated to filesystem management
     * Unlike outer class 'DocumentCollection', that class manages fs events/watch service, objects nesting
     */
    private inner class FileWatcher {
        private val watchService: WatchService = FileSystems.getDefault().newWatchService()

        // using concurrent hash map is not necessary in current situation because we always access that maps from single-threaded coroutine context
        // but if is safer to switch now
        private val directories = ConcurrentHashMap<Path, WatchKey>()
        private val key2dir = ConcurrentHashMap<WatchKey, Path>()

        private val directoryContent = ConcurrentHashMap<Path, DirectoryContent>()

        private fun getInnerElements(absoluteDirPath: Path): DirectoryContent {
            return directoryContent.getOrPut(absoluteDirPath) { DirectoryContent() }
        }

        private fun canonicalizePath(path: Path): Path {
            return path.absolute().normalize()
        }

        private fun createFile(absolutePath: Path) {
            getInnerElements(absolutePath.parent).addFile(absolutePath)
            scope.launch {
                startWorker {
                    indexDocument(absolutePath, this)
                }
            }
        }

        private fun deleteFile(absolutePath: Path) {
            if (getInnerElements(absolutePath.parent).removeFile(absolutePath)) {
                scope.launch {
                    startWorker {
                        deleteDocument(absolutePath, this)
                    }
                }
            }
        }

        private suspend fun modifyFile(absolutePath: Path) {
            scope.launch {
                startWorker {
                    indexDocument(absolutePath, this)
                }
            }
        }

        private fun registerDir(absolutePath: Path) {
            if (!directories.containsKey(absolutePath)) {
                val key = absolutePath.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE
                )
                directories[absolutePath] = key
                key2dir[key] = absolutePath
                getInnerElements(absolutePath.parent).addDir(absolutePath)
            }
        }

        private fun unregisterDir(absolutePath: Path) {
            val key = directories.remove(absolutePath)
            if (key != null) {
                key2dir.remove(key)
                key.cancel()
            }
            getInnerElements(absolutePath.parent).removeDir(absolutePath)
        }

        suspend fun registerDirTree(path: Path): RegisterDirResult {
            val res = scope.async {
                val absolutePath = canonicalizePath(path)
                if (directories.containsKey(absolutePath)) {
                    RegisterDirResult.AlreadyWatched
                } else {
                    try {
                        withContext(Dispatchers.IO) {
                            if (!absolutePath.isDirectory()) {
                                throw Exception("$path is not a directory")
                            }
                            Files.walkFileTree(absolutePath, object : SimpleFileVisitor<Path>() {
                                override fun preVisitDirectory(
                                    subPath: Path,
                                    attrs: BasicFileAttributes
                                ): FileVisitResult {
                                    val absolute = canonicalizePath(subPath)
                                    return if (directories.containsKey(absolute)) {
                                        FileVisitResult.SKIP_SUBTREE
                                    } else {
                                        registerDir(absolute)
                                        FileVisitResult.CONTINUE
                                    }
                                }

                                override fun visitFile(file: Path?, attrs: BasicFileAttributes?): FileVisitResult {
                                    val absolute = canonicalizePath(file!!)
                                    createFile(absolute)
                                    return FileVisitResult.CONTINUE
                                }
                            })
                        }
                        RegisterDirResult.Ok
                    } catch (ex: Exception) {
                        RegisterDirResult.Error(ex)
                    }
                }
            }
            return res.await()
        }

        suspend fun unregisterDirTree(path: Path): UnregisterDirResult {
            val absolutePath = canonicalizePath(path)
            val parent = absolutePath.parent
            if (!directories.containsKey(absolutePath)) {
                return UnregisterDirResult.WasNotWatched
            }
            if (directories.containsKey(parent)) {
                return UnregisterDirResult.ParentWatched
            } else {
                return scope.async {
                    try {
                        // "watch" coroutine and external call to unregister directory could lead to inconsistent state of `nesting` structure,
                        // which would result in insufficient unsubscribing from watch
                        // it would be redone completely with new index structure
                        val nestedItems =
                            bfs(
                                absolutePath,
                                { directoryContent[it]?.dirs?.toList() ?: emptyList() },
                                { directoryContent[it]?.files?.toList() ?: emptyList() })
                        for ((dir, files) in nestedItems) {
                            unregisterDir(dir)
                            for (f in files) {
                                deleteFile(f)
                            }
                        }
                        UnregisterDirResult.Ok
                    } catch (ex: Exception) {
                        UnregisterDirResult.Error(ex)
                    }
                }.await()
            }
        }

        suspend fun watch() {
            withContext(Dispatchers.IO) {
                while (isActive) {
                    val watchKey = watchService.poll(100, TimeUnit.MILLISECONDS) ?: continue

                    val path = key2dir[watchKey]

                    val watchEvents = watchKey.pollEvents()
                    watchKey.reset()

                    if (path != null) {
                        for (event in watchEvents) {
                            val kind = event.kind()

                            val name: Path = event.context() as Path
                            val absolute = canonicalizePath(path.resolve(name))

                            val f = absolute.toFile()

                            when (kind) {
                                StandardWatchEventKinds.ENTRY_CREATE -> {
                                    when {
                                        f.isFile -> createFile(absolute)
                                        f.isDirectory -> registerDirTree(absolute)
                                    }
                                }

                                StandardWatchEventKinds.ENTRY_MODIFY -> {
                                    when {
                                        f.isFile -> modifyFile(absolute)
                                        f.isDirectory -> {}
                                    }
                                }

                                StandardWatchEventKinds.ENTRY_DELETE -> {
                                    if (directories.containsKey(absolute)) {
                                        unregisterDirTree(absolute)
                                    } else {
                                        deleteFile(absolute)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}