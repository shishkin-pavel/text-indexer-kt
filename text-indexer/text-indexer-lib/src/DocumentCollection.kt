//import kotlinx.coroutines.*
//import java.io.File
//import java.nio.file.*
//import java.nio.file.StandardWatchEventKinds.*
//import java.nio.file.attribute.BasicFileAttributes
//import java.util.*
//import java.util.concurrent.ConcurrentHashMap
//import java.util.concurrent.TimeUnit
//import kotlin.io.path.absolute
//import kotlin.io.path.isDirectory
//
////import io.github.oshai.kotlinlogging.KotlinLogging
//
////val logger = KotlinLogging.logger {}
//
//fun <T, TData, TColl : Collection<T>> bfs(
//    start: T,
//    getChildren: (T) -> TColl,
//    getData: (T) -> TData
//): List<Pair<T, TData>> {
//    val queue: Queue<T> = LinkedList()
//    val result = mutableListOf<Pair<T, TData>>()
//    val visited = mutableSetOf<T>()
//
//    queue.add(start)
//    visited.add(start)
//
//    while (queue.isNotEmpty()) {
//        val current = queue.poll()
//        result.add(Pair(current, getData(current)))
//
//        for (c in getChildren(current)) {
//            if (!visited.contains(c)) {
//                queue.add(c)
//                visited.add(c)
//            }
//        }
//    }
//
//    return result
//}
//
//sealed class RegisterDirResult {
//    data object Ok : RegisterDirResult()
//    data object AlreadyWatched : RegisterDirResult()
//    data class Error(val ex: Exception) : RegisterDirResult()
//
//}
//
//sealed class UnegisterDirResult {
//    data object Ok : UnegisterDirResult()
//    data object ParentWatched : UnegisterDirResult()
//    data object WasNotWatched : UnegisterDirResult()
//    data class Error(val ex: Exception) : UnegisterDirResult()
//
//}
//
//@OptIn(DelicateCoroutinesApi::class)
//class DocumentCollection<TPos>(
//    private val tokenizer: Tokenizer<TPos>,
//    private val emptyIndex: () -> Index<TPos>,
//    private val scope: CoroutineScope
//) : AutoCloseable {
//    // switched to String instead of Path because not every pair paths with the same string-path are equal
//    // it probably would be a problem when links will occur
//    private val documents = ConcurrentHashMap<String, Document<TPos>>()
//
//    // dedicated thread for filesystem events as soon as we block on querying events
//    private val fileWatcherContext = newSingleThreadContext("file watcher thread")
//    private val fileWatcher = FileWatcher()
//
//    private val fileWatcherJob: Job = scope.launch {
//        coroutineScope {
//            launch(fileWatcherContext) {
//                fileWatcher.watch()
//            }
//        }
//    }
//
//    suspend fun query(str: String): List<Pair<Path, ArrayList<TPos>>> {
//        return coroutineScope {
//            async(Dispatchers.Default) {
//                documents.values.map {
//                    async {
//                        Pair(it.file.toPath(), it.queryString(str))
//                    }
//                }.map {
//                    it.await()
//                }
//            }
//        }.await()
//    }
//
//    suspend fun waitForIndexFinish() {
////        logger.info { "waiting for ${documents.size} indexes" }
//        val indexes = coroutineScope {
//            documents.values.map {
//                async { it.getIndex() }
//            }.map { it.await() }
//        }
//    }
//
//    suspend fun registerDir(path: Path): RegisterDirResult {
//        return fileWatcher.registerDirTree(path)
//    }
//
//    suspend fun unregisterDir(path: Path): UnegisterDirResult {
//        return fileWatcher.unregisterDirTree(path)
//    }
//
//    override fun close() {
////        logger.info { "cleaning up..." }
//        fileWatcherJob.cancel()
//        for (d in documents.values) {
//            d.close()
//        }
////        logger.info { "cleaning up finished" }
//    }
//
//    private data class NestedItems(val dirs: HashSet<Path>, val files: HashSet<Path>) {
//        constructor() : this(HashSet(), HashSet())
//
//        fun addDir(absolutePath: Path) {
//            dirs.add(absolutePath)
//        }
//
//        fun removeDir(absolutePath: Path) {
//            dirs.remove(absolutePath)
//        }
//
//        fun addFile(absolutePath: Path) {
//            files.add(absolutePath)
//        }
//
//        fun removeFile(absolutePath: Path): Boolean {
//            return files.remove(absolutePath)
//        }
//    }
//
//    /**
//     * Class dedicated to filesystem management
//     * Unlike outer class 'DocumentCollection', that class manages fs events/watch service, objects nesting
//     */
//    private inner class FileWatcher {
//        private val watchService: WatchService = FileSystems.getDefault().newWatchService()
//
//        // using concurrent hash map is not necessary in current situation because we always access that maps from single-threaded coroutine context
//        // but if is safer to switch now
//        private val directories = ConcurrentHashMap<Path, WatchKey>()
//        private val key2dir = ConcurrentHashMap<WatchKey, Path>()
//
//        private val nesting = ConcurrentHashMap<Path, NestedItems>()
//
//        private fun getInnerElements(absoluteDirPath: Path): NestedItems {
//            return nesting.getOrPut(absoluteDirPath) { NestedItems() }
//        }
//
//        private fun canonicalizePath(path: Path): Path {
//            return path.absolute().normalize()
//        }
//
//        private fun createFileHelper(absolutePath: Path): Document<TPos> {
//            getInnerElements(absolutePath.parent).addFile(absolutePath)
//            return Document(File(absolutePath.toString()), tokenizer, emptyIndex, scope)
//        }
//
//        private fun createFile(absolutePath: Path) {
//            documents.computeIfAbsent(absolutePath.toString()) {
//                createFileHelper(absolutePath)
//            }
//        }
//
//        private fun deleteFile(absolutePath: Path) {
//            if (getInnerElements(absolutePath.parent).removeFile(absolutePath)) {
//                val doc = documents.remove(absolutePath.toString())
//                doc?.close()
//            }
//        }
//
//        private suspend fun modifyFile(absolutePath: Path) {
//            var newlyCreated = false
//            val doc = documents.getOrPut(absolutePath.toString()) {
//                newlyCreated = true
//                createFileHelper(absolutePath)
//            }
//            if (!newlyCreated) {
//                doc.rebuildIndex()
//            }
//        }
//
//        private fun registerDir(absolutePath: Path) {
//            if (!directories.containsKey(absolutePath)) {
//                val key = absolutePath.register(
//                    watchService,
//                    ENTRY_CREATE,
//                    ENTRY_MODIFY,
//                    ENTRY_DELETE
//                )
//                directories[absolutePath] = key
//                key2dir[key] = absolutePath
//                getInnerElements(absolutePath.parent).addDir(absolutePath)
//            }
//        }
//
//        private fun unregisterDir(absolutePath: Path) {
//            val key = directories.remove(absolutePath)
//            if (key != null) {
//                key2dir.remove(key)
//                key.cancel()
//            }
//            getInnerElements(absolutePath.parent).removeDir(absolutePath)
//        }
//
//        suspend fun registerDirTree(path: Path): RegisterDirResult {
//            val res = scope.async {
//                val absolutePath = canonicalizePath(path)
//                if (directories.containsKey(absolutePath)) {
//                    RegisterDirResult.AlreadyWatched
//                } else {
//                    try {
//                        withContext(Dispatchers.IO) {
//                            if (!absolutePath.isDirectory()) {
//                                throw Exception("$path is not a directory")
//                            }
//                            Files.walkFileTree(absolutePath, object : SimpleFileVisitor<Path>() {
//                                override fun preVisitDirectory(
//                                    subPath: Path,
//                                    attrs: BasicFileAttributes
//                                ): FileVisitResult {
//                                    val absolute = canonicalizePath(subPath)
//                                    return if (directories.containsKey(absolute)) {
//                                        FileVisitResult.SKIP_SUBTREE
//                                    } else {
//                                        registerDir(absolute)
//                                        FileVisitResult.CONTINUE
//                                    }
//                                }
//
//                                override fun visitFile(file: Path?, attrs: BasicFileAttributes?): FileVisitResult {
//                                    val absolute = canonicalizePath(file!!)
//                                    createFile(absolute)
//                                    return FileVisitResult.CONTINUE
//                                }
//                            })
//                        }
//                        RegisterDirResult.Ok
//                    } catch (ex: Exception) {
//                        RegisterDirResult.Error(ex)
//                    }
//                }
//            }
//            return res.await()
//        }
//
//        suspend fun unregisterDirTree(path: Path): UnegisterDirResult {
//            val absolutePath = canonicalizePath(path)
//            val parent = absolutePath.parent
//            if (!directories.containsKey(absolutePath)) {
//                return UnegisterDirResult.WasNotWatched
//            }
//            if (directories.containsKey(parent)) {
//                return UnegisterDirResult.ParentWatched
//            } else {
//                return scope.async {
//                    try {
//                        // "watch" coroutine and external call to unregister directory could lead to inconsistent state of `nesting` structure,
//                        // which would result in insufficient unsubscribing from watch
//                        // it would be redone completely with new index structure
//                        val nestedItems =
//                            bfs(
//                                absolutePath,
//                                { nesting[it]?.dirs?.toList() ?: emptyList() },
//                                { nesting[it]?.files?.toList() ?: emptyList() })
//                        for ((dir, files) in nestedItems) {
//                            unregisterDir(dir)
//                            for (f in files) {
//                                deleteFile(f)
//                            }
//                        }
//                        UnegisterDirResult.Ok
//                    } catch (ex: Exception) {
//                        UnegisterDirResult.Error(ex)
//                    }
//                }.await()
//            }
//        }
//
//        suspend fun watch() {
//            withContext(Dispatchers.IO) {
//                while (isActive) {
//                    val watchKey = watchService.poll(100, TimeUnit.MILLISECONDS) ?: continue
//
//                    val path = key2dir[watchKey]
//
//                    val watchEvents = watchKey.pollEvents()
//                    watchKey.reset()
//
//                    if (path != null) {
//                        for (event in watchEvents) {
//                            val kind = event.kind()
//
//                            val name: Path = event.context() as Path
//                            val absolute = canonicalizePath(path.resolve(name))
//
//                            val f = absolute.toFile()
//
//                            when (kind) {
//                                ENTRY_CREATE -> {
//                                    when {
//                                        f.isFile -> createFile(absolute)
//                                        f.isDirectory -> registerDirTree(absolute)
//                                    }
//                                }
//
//                                ENTRY_MODIFY -> {
//                                    when {
//                                        f.isFile -> modifyFile(absolute)
//                                        f.isDirectory -> {}
//                                    }
//                                }
//
//                                ENTRY_DELETE -> {
//                                    if (directories.containsKey(absolute)) {
//                                        unregisterDirTree(absolute)
//                                    } else {
//                                        deleteFile(absolute)
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
//}