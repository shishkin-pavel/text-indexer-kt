import java.nio.file.*
import kotlin.io.path.absolute
import kotlin.io.path.isRegularFile

class FileWatcher {
    val watchService = FileSystems.getDefault().newWatchService()
    val directories = HashMap<Path, WatchKey>()
    val key2dir = HashMap<WatchKey, Path>()
    val files = HashMap<Path, Document>()

    fun watchDirectoryTree(rootPath: Path, trustFileAttrs: Boolean) {
        fun registerFile(path: Path) {
            if (!files.containsKey(path)) {
                val file = path.toFile()
                val doc = Document(file)
                files[path] = doc
                println("file registered $path")
            }
        }

        fun registerDir(path: Path) {
            if (!directories.containsKey(path)) {
                val key = path.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE
                )
                directories[path] = key
                key2dir[key] = path
                println("dir registered $path")
            }
        }

        fun registerDirTree(path: Path) {
            Files.walk(path).forEach {
                val absolute = it.absolute()
                if (Files.isDirectory(it)) {
                    registerDir(absolute)
                } else if (Files.isRegularFile(it)) {
                    registerFile(absolute);
                }
            }
        }

        fun unregisterFile(absolutePath: Path) {
            println("file $absolutePath was deleted")
            val doc = files.remove(absolutePath)!!
            doc.close()
        }

        fun unregisterDir(absolutePath: Path) {
            println("dir  $absolutePath was deleted")
            val key = directories.remove(absolutePath)
            key2dir.remove(key)
        }

        fun unregister(absolutePath: Path) {
            if (files.containsKey(absolutePath)) {
                unregisterFile(absolutePath)
            } else if (directories.containsKey(absolutePath)) {
                unregisterDir(absolutePath)
            }
        }

        registerDirTree(rootPath)

        while (true) {
            val watchKey: WatchKey = watchService.take()
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
                            registerFile(absolute)
                        }
                    }

                    StandardWatchEventKinds.ENTRY_MODIFY -> {
                        if (absolute.isRegularFile()) {
                            if (trustFileAttrs) {
                                // TODO file attrs comparison
                            }
                            val doc = files[absolute]!!
                            doc.rebuildIndex()
                        }
                        println("$absolute was modified")
                    }

                    StandardWatchEventKinds.ENTRY_DELETE -> unregister(absolute)

                }
            }

            watchKey.reset()
        }
    }
}