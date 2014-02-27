package schema.registry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

import com.google.common.base.Splitter;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class FilterStreamUtil {

    private static Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();

    public static InputStream filter(InputStream in, String filters) throws IOException {
        if (filters == null || (filters = filters.trim()).isEmpty()) {
            return in;
        }

        for (String filter : parseFilters(filters)) {
            String s = filter.toLowerCase();
            switch (s) {
                case "base64":
                    in = new Base64InputStream(in);
                    break;
                case "base64raw":
                    in = new Base64InputStream(in, false, 0, null);
                    break;
                case "bzip2":
                    in = new BZip2CompressorInputStream(in);
                    break;
                case "deflate":
                    in = new InflaterInputStream(in);
                    break;
                case "gzip":
                    in = new GZIPInputStream(in);
                    break;
                case "lz4":
                    in = new LZ4BlockInputStream(in);
                    break;
                case "lzf":
                    in = new LZFInputStream(in);
                    break;
                case "snappy":
                    in = new SnappyInputStream(in);
                    break;
                default:
                    if (s.startsWith("skip")) {
                        int n = Integer.parseInt(s.substring(4));
                        if (n > 0) {
                            for (int i = 0; i < n; ++i) {
                                in.read();
                            }
                        } else if (n == 0) {
                            while (true) {
                                int i = in.read();
                                if (i == 0 || i == -1) {
                                    break;
                                }
                            }
                        }
                    }

                    throw new IllegalArgumentException("unknown filter " + filter);
            }
        }

        return in;
    }

    public static OutputStream filter(OutputStream out, String filters) throws IOException {
        if (filters == null || (filters = filters.trim()).isEmpty()) {
            return out;
        }

        for (String filter : parseFilters(filters)) {
            switch (filter.toLowerCase()) {
                case "base64":
                    out = new Base64OutputStream(out);
                    break;
                case "base64raw":
                    out = new Base64OutputStream(out, true, 0, null);
                    break;
                case "bzip2":
                    out = new BZip2CompressorOutputStream(out);
                    break;
                case "deflate":
                    out = new DeflaterOutputStream(out);
                    break;
                case "gzip":
                    out = new GZIPOutputStream(out);
                    break;
                case "lz4":
                    out = new LZ4BlockOutputStream(out);
                    break;
                case "lzf":
                    out = new LZFOutputStream(out);
                    break;
                case "snappy":
                    out = new SnappyOutputStream(out);
                    break;
                default:
                    throw new IllegalArgumentException("unknown filter " + filter);
            }
        }

        return out;
    }

    private static Iterable<String> parseFilters(String filters) {
        return splitter.split(filters);
    }
}
