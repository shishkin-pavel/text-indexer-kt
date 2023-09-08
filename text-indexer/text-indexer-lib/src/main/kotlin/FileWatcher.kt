import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withContext
import java.nio.file.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.HashSet
import kotlin.io.path.absolute

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

data class FileWatchEvent(
    val path: Path,
    val kind: Kind,
) {
    enum class Kind {
        Created,
        Modified,
        Deleted
    }
}

class FileWatcher {
    private val watchService: WatchService = FileSystems.getDefault().newWatchService()
    val eventChannel = Channel<FileWatchEvent>()

    // using concurrent hash map is not necessary in current situation because we always access that maps from single-threaded coroutine context
    // but if is safer to switch now
    private val directories = ConcurrentHashMap<Path, WatchKey>()
    private val key2dir = ConcurrentHashMap<WatchKey, Path>()

    private data class NestedItems(val dirs: HashSet<Path>, val files: HashSet<Path>) {
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

    private val nesting = ConcurrentHashMap<Path, NestedItems>()

    private fun getInnerElements(absoluteDirPath: Path): NestedItems {
        return nesting.getOrPut(absoluteDirPath) { NestedItems() }
    }

    private suspend fun fileCreated(absolutePath: Path) {
        getInnerElements(absolutePath.parent).addFile(absolutePath)
        eventChannel.send(FileWatchEvent(absolutePath, FileWatchEvent.Kind.Created))
    }

    private suspend fun fileDeleted(absolutePath: Path) {
        if (getInnerElements(absolutePath.parent).removeFile(absolutePath)) {
            eventChannel.send(FileWatchEvent(absolutePath, FileWatchEvent.Kind.Deleted))
        }
    }

    private suspend fun fileModified(absolutePath: Path) {
        eventChannel.send(FileWatchEvent(absolutePath, FileWatchEvent.Kind.Modified))
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

    private suspend fun unregisterDirTree(absolutePath: Path) {
        val nestedItems =
            bfs(
                absolutePath,
                { nesting[it]?.dirs?.toList() ?: emptyList() },
                { nesting[it]?.files?.toList() ?: emptyList() })
        for ((dir, files) in nestedItems) {
            unregisterDir(dir)
            for (f in files) {
                fileDeleted(f)
            }
        }
    }

    private suspend fun registerDirTree(absolutePath: Path) {
        withContext(Dispatchers.IO) {
            val paths = Files.walk(absolutePath).toList()
            paths.forEach { path ->
                val absolute = path.absolute()
                when {
                    Files.isDirectory(absolute) -> registerDir(absolute)
                    Files.isRegularFile(absolute) -> {
                        fileCreated(absolute)
                    }
                }
            }
        }
    }

    suspend fun watchDirectoryTree(rootPath: Path) {
        withContext(Dispatchers.IO) {
            @Suppress("NAME_SHADOWING") val rootPath = rootPath.absolute()
            registerDirTree(rootPath)

            while (true) {
                val watchKey: WatchKey =
                    watchService.take()

                val path = key2dir[watchKey]

                val watchEvents = watchKey.pollEvents()
                watchKey.reset()

                if (path != null) {
                    for (event in watchEvents) {
                        val kind = event.kind()

                        val name: Path = event.context() as Path
                        val absolute: Path = path.resolve(name)

                        val f = absolute.toFile()

                        when (kind) {
                            StandardWatchEventKinds.ENTRY_CREATE -> {
                                when {
                                    f.isFile -> fileCreated(absolute)
                                    f.isDirectory -> registerDirTree(absolute)
                                }
                            }

                            StandardWatchEventKinds.ENTRY_MODIFY -> {
                                when {
                                    f.isFile -> fileModified(absolute)
                                    f.isDirectory -> {}
                                }
                            }

                            StandardWatchEventKinds.ENTRY_DELETE -> {
                                if (directories.containsKey(absolute)) {
                                    unregisterDirTree(absolute)
                                } else {
                                    fileDeleted(absolute)
                                }
                            }

                        }
                    }
                }
            }
        }
    }
}