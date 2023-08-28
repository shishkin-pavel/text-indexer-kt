import org.mozilla.universalchardet.UniversalDetector
import java.io.File
import java.io.FileInputStream
import java.lang.Exception
import java.nio.charset.Charset

class Document(val file: File) {
    val charset: Charset

    fun detectCharset(): String? {
        val buf = ByteArray(4096)
        val fis = FileInputStream(file)
        val detector = UniversalDetector(null)

        var nread: Int
        while (fis.read(buf).also { nread = it } > 0 && !detector.isDone) {
            detector.handleData(buf, 0, nread)
        }

        detector.dataEnd()
        val encoding = detector.detectedCharset
        detector.reset()

        fis.close()
        return encoding
    }

    init {
        val detectedCharset = detectCharset()
        val charset = Charset.availableCharsets()[detectedCharset]
        if (charset == null) {
            throw Exception("no suitable charset found for $detectedCharset")
        } else {
            this.charset = charset
        }
    }

    fun test() {
    }
}