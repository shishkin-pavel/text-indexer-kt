import kotlinx.coroutines.*
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
class FileWatcher(
    private val onCreate: (path: Path) -> Unit,
    private val onModification: (path: Path) -> Unit,
    private val onDelete: (path: Path) -> Unit,
    private val scope: CoroutineScope
) : AutoCloseable {
    @OptIn(DelicateCoroutinesApi::class)
    private val fileWatcherContext = newSingleThreadContext("file watcher thread")
    private val watchService: WatchService = FileSystems.getDefault().newWatchService()
    private val fileWatcherJob: Job = scope.launch(CoroutineName("fileWatcher") + fileWatcherContext) {
//        coroutineScope {
//            launch(fileWatcherContext) {
        watch()
//            }
//        }
    }

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
        onCreate(absolutePath)
    }

    private fun deleteFile(absolutePath: Path) {
        directoryContent[absolutePath.parent]?.removeFile(absolutePath)
        onDelete(absolutePath)

    }

    private fun modifyFile(absolutePath: Path) {
        onModification(absolutePath)
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
        directoryContent[absolutePath.parent]?.removeDir(absolutePath)
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
                        RegisterDirResult.Ok
                    }
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

    private suspend fun watch() {
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

    override fun close() {
        fileWatcherJob.cancel()
    }
}