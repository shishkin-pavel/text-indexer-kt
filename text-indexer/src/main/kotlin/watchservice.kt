// borrowed from https://proandroiddev.com/kotlin-watchservice-a-better-file-watcher-using-channels-coroutines-and-sealed-classes-7ab5c9df3ada
// and fixed (wasn't working properly)

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import java.io.File
import java.nio.file.*
import java.nio.file.WatchKey
import java.nio.file.FileVisitResult
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.SimpleFileVisitor
import java.nio.file.Files
import java.nio.file.StandardWatchEventKinds.*

fun File.asWatchChannel(
    scope: CoroutineScope = GlobalScope
) = KWatchChannel(
    file = this,
    scope = scope,
)

class KWatchChannel(
    val file: File,
    val scope: CoroutineScope = GlobalScope,
    private val channel: Channel<KWatchEvent> = Channel()
) : Channel<KWatchEvent> by channel {

    private val watchService: WatchService = FileSystems.getDefault().newWatchService()
    private val registeredKeys = ArrayList<WatchKey>()
    private val path: Path = if (file.isFile) {
        file.parentFile
    } else {
        file
    }.toPath()

    private fun registerPaths() {
        registeredKeys.apply {
            forEach { it.cancel() }
            clear()
        }

        Files.walkFileTree(path, object : SimpleFileVisitor<Path>() {
            override fun preVisitDirectory(subPath: Path, attrs: BasicFileAttributes): FileVisitResult {
                registeredKeys += subPath.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE)
                return FileVisitResult.CONTINUE
            }
        })
    }

    init {
        // commence emitting events from channel
        scope.launch(Dispatchers.IO) {

            // sending channel initalization event
            channel.send(
                KWatchEvent(
                    file = path.toFile(),
                    kind = KWatchEvent.Kind.Initialized
                )
            )

            var shouldRegisterPath = true

            while (!isClosedForSend) {

                if (shouldRegisterPath) {
                    registerPaths()
                    shouldRegisterPath = false
                }

                val monitorKey = watchService.take()
                val dirPath = monitorKey.watchable() as? Path ?: break
                monitorKey.pollEvents().forEach {
                    val eventPath = dirPath.resolve(it.context() as Path)

                    val eventType = when (it.kind()) {
                        ENTRY_CREATE -> KWatchEvent.Kind.Created
                        ENTRY_DELETE -> KWatchEvent.Kind.Deleted
                        else -> KWatchEvent.Kind.Modified
                    }

                    val event = KWatchEvent(
                        file = eventPath.toFile(),
                        kind = eventType
                    )

                    // if any folder is created or deleted... and we are supposed
                    // to watch subtree we re-register the whole tree
                    if (event.kind in listOf(KWatchEvent.Kind.Created, KWatchEvent.Kind.Deleted) &&
                        event.file.isDirectory
                    ) {
                        shouldRegisterPath = true
                    }

                    channel.send(event)
                }

                if (!monitorKey.reset()) {
                    monitorKey.cancel()
                    close()
                    break
                } else if (isClosedForSend) {
                    break
                }
            }
        }
    }

    override fun close(cause: Throwable?): Boolean {
        registeredKeys.apply {
            forEach { it.cancel() }
            clear()
        }

        return channel.close(cause)
    }
}

/**
 * Wrapper around [WatchEvent] that comes with properly resolved absolute path
 */
data class KWatchEvent(
    val file: File,
    val kind: Kind,
) {
    enum class Kind(val kind: String) {
        Initialized("initialized"),
        Created("created"),
        Modified("modified"),
        Deleted("deleted")
    }
}