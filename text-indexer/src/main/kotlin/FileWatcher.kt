import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withContext
import java.nio.file.*
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
    private val directories = HashMap<Path, WatchKey>()
    private val key2dir = HashMap<WatchKey, Path>()
    val eventChannel = Channel<FileWatchEvent>()

    private suspend fun fileCreated(absolutePath: Path) {
        eventChannel.send(FileWatchEvent(absolutePath, FileWatchEvent.Kind.Created))
        println("file $absolutePath was created")
    }

    private suspend fun fileDeleted(absolutePath: Path) {
        eventChannel.send(FileWatchEvent(absolutePath, FileWatchEvent.Kind.Deleted))
        println("file $absolutePath was deleted")
    }

    private suspend fun fileModified(absolutePath: Path) {
        eventChannel.send(FileWatchEvent(absolutePath, FileWatchEvent.Kind.Modified))
        println("file $absolutePath was modified")
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
            println("dir registered $absolutePath")
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
        println("dir  $absolutePath was deleted")
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

                    when (kind) {
                        StandardWatchEventKinds.ENTRY_CREATE -> {
                            println("$absolute was created")
                            if (Files.isDirectory(absolute, LinkOption.NOFOLLOW_LINKS)) {
                                registerDirTree(absolute)
                            } else if (Files.isRegularFile(absolute)) {
                                fileCreated(absolute)
                            }
                        }

                        StandardWatchEventKinds.ENTRY_MODIFY -> {
                            when {
                                Files.isRegularFile(absolute) -> fileModified(absolute)
                                Files.isDirectory(absolute) -> {}
                            }
                        }

                        StandardWatchEventKinds.ENTRY_DELETE -> {
                            when {
                                Files.isRegularFile(absolute) -> fileDeleted(absolute)
                                Files.isDirectory(absolute) -> unregisterDir(absolute)
                            }
                        }

                    }
                }

                watchKey.reset()
            }
        }
    }
}