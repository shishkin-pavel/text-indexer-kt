import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withContext
import java.nio.file.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.absolute

data class FileWatchEvent(
    val path: Path,
    val kind: Kind,
) {
    enum class Kind(val kind: String) {
        Created("created"),
        Modified("modified"),
        Deleted("deleted")
    }
}

class FileWatcher {
    private val watchService: WatchService = FileSystems.getDefault().newWatchService()
    val eventChannel = Channel<FileWatchEvent>()

    // using concurrent hash map is not necessary in current situation because we always access that maps from single-threaded coroutine context
    // but if is safer to switch now
    private val directories = ConcurrentHashMap<Path, WatchKey>()
    private val key2dir = ConcurrentHashMap<WatchKey, Path>()

    private suspend fun fileCreated(absolutePath: Path) {
        eventChannel.send(FileWatchEvent(absolutePath, FileWatchEvent.Kind.Created))
    }

    private suspend fun fileDeleted(absolutePath: Path) {
        eventChannel.send(FileWatchEvent(absolutePath, FileWatchEvent.Kind.Deleted))
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

    private fun unregisterDir(absolutePath: Path) {
        val key = directories.remove(absolutePath)
        key2dir.remove(key)
    }

    suspend fun watchDirectoryTree(rootPath: Path) {
        withContext(Dispatchers.IO) {
            @Suppress("NAME_SHADOWING") val rootPath = rootPath.absolute()
            registerDirTree(rootPath)

            while (true) {
                val watchKey: WatchKey =
                    watchService.take()

                val path = key2dir[watchKey]
                for (event in watchKey.pollEvents()) {
                    val kind = event.kind()

                    val name: Path = event.context() as Path
                    val absolute: Path =
                        path!!.resolve(name)   // TODO dir delete event can happen earlier than events representing nested file deletion, hence npe will be thrown here

                    val f = absolute.toFile()

                    when (kind) {
                        StandardWatchEventKinds.ENTRY_CREATE -> {
                            when {
                                f.isFile -> fileCreated(absolute)
                                f.isDirectory -> registerDirTree(absolute)
                            }
                        }

                        StandardWatchEventKinds.ENTRY_MODIFY -> {
                            absolute.toFile().isFile
                            when {
                                f.isFile -> fileModified(absolute)
                                f.isDirectory -> {}
                            }
                        }

                        StandardWatchEventKinds.ENTRY_DELETE -> {
                            if (directories.containsKey(absolute)) {
                                unregisterDir(absolute)
                            } else {
                                fileDeleted(absolute)
                            }
                        }

                    }
                }

                watchKey.reset()
            }
        }
    }
}