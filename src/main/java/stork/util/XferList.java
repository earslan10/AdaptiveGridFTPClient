package stork.util;

import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.CooperativeChannels.Density;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class XferList implements Iterable<XferList.Entry> {
  private final Entry root;
  public String sp, dp;
  public double totalTransferredSize = 0, instantTransferredSize = 0;
  public int totalTransferredCount = 0, instantTransferredCount = 0;
  public double estimatedFinishTime = 0;
  public double instant_throughput = 0, overall_throughput = 0, weighted_throughput = 0;
  public long initialSize = 0;
  public List<Integer> channels;
  public int[] params;
  public int interval = 0;
  public int onAir = 0;
  public boolean isReadyToTransfer = false;
  public Density density;
  public int parallelism = -1, pipelining = -1, concurrency = -1, bufferSize = -1;
  private LinkedList<Entry> list = new LinkedList<Entry>();
  private long size = 0;
  private int count = 0;  // Number of files (not dirs)

  // Create an XferList for a directory.
  public XferList(String src, String dest) {
    if (!src.endsWith("/")) {
      src += "/";
    }
    if (!dest.endsWith("/")) {
      dest += "/";
    }
    sp = src;
    dp = dest;
    root = new Entry("");
  }

  // Create an XferList for a file.
  public XferList(String src, String dest, long size) {
    sp = src;
    dp = dest;
    root = new Entry("", size);
    list.add(root);
    this.size += size;
  }

  public void setTransferParameters(int[] parameters) {
    concurrency = parameters[0];
    parallelism = parameters[1];
    pipelining = parameters[2];
    bufferSize = parameters[3];
  }

  // Add a directory to the list.
  public void add(String path) {
    list.add(new Entry(path));
  }

  // Add a file to the list.
  public void add(String path, long size) {
    list.add(new Entry(path, size));
    this.size += size;
    this.count++;
  }

  void addEntry(Entry e) {
    list.add(e);
    if (e.len == -1) {
      size += e.remaining();
    } else {
      size += e.len;
    }
    count++;
  }

  // Add another XferList's entries under this XferList.
  public void addAll(XferList ol) {
    size += ol.size;
    count += ol.count;
    for (Entry e : ol)
      list.add(new Entry(e.path, e.size));
    //list.addAll(ol.list);
  }

  public long size() {
    return size;
  }

  public int count() {
    return count;
  }

  public boolean isEmpty() {
    return list.isEmpty();
  }

  // Remove and return the topmost entry.
  public Entry pop() {
    try {
      Entry e = list.pop();
      size -= e.size;
      count--;
      return e;
    } catch (Exception e) {
      return null;
    }
  }

  public Entry getItem(int index) {
    try {
      Entry e = list.get(index);
      return e;
    } catch (Exception e) {
      return null;
    }
  }

  public void removeItem(int index) {
    try {
      Entry e = list.get(index);
      size -= e.size;
      list.remove(index);
    } catch (Exception e) {

    }
  }

  public void removeAll() {
    try {
      while (!list.isEmpty())
        list.removeFirst();
      count = 0;
      size = 0;
    } catch (Exception e) {

    }
  }

  // Get the progress of the list in terms of bytes.
  public Progress byteProgress() {
    Progress p = new Progress();

    for (Entry e : list)
      p.add(e.remaining(), e.size);
    return p;
  }

  // Get the progress of the list in terms of files.
  public Progress fileProgress() {
    Progress p = new Progress();

    for (Entry e : list)
      p.add(e.done ? 1 : 0, 1);
    return p;
  }

  // Split off a sublist from the front of this list which is
  // of a certain byte length.
  public XferList split(long len) {
    XferList nl = new XferList(sp, dp);
    Iterator<Entry> iter = iterator();

    if (len == -1 || size <= len) {
      // If the request is bigger than the list, empty into new list.
      nl.list = list;
      nl.size = size;
      list = new LinkedList<Entry>();
      size = 0;
    } else {
      while (len > 0 && iter.hasNext()) {
        Entry e2, e = iter.next();

        if (e.done) {
          iter.remove();
        } else if (e.dir || e.size() <= len || e.size() - len < (ConfigurationParams.MAXIMUM_SINGLE_FILE_SIZE / 5)) {
          nl.addEntry(new Entry(e.path, e));
          len -= e.size();
          iter.remove();
          count--;
          size -= e.size();
          e.done = true;
        } else {  // Need to split file...
          e2 = new Entry(e.path, e);
          e2.len = len;
          nl.addEntry(e2);
          size -= e2.len;
          if (e.len == -1) {
            e.len = e.size;
          }
          e.len -= len;
          e.off += len;
          len = 0;
        }
      }
    }
    return nl;
  }

  public XferList sliceLargeFiles(long sliceSize) {
    Iterator<Entry> iter = iterator();
    XferList nl = new XferList(sp, dp);
    while (iter.hasNext()) {
      Entry e2, e = iter.next();
      if (e.size() > sliceSize) {
        int pieceCount = (int) Math.ceil(1.0 * e.size / sliceSize);
        long pieceSize = (int) (e.size / pieceCount);
        long offset = 0;
        //System.out.println("Removing "+ e.path() + " " + e.off + " size:"+ e.remaining() + " " + e.len + " to insert "+ pieceCount + "pieces" + " each" + pieceSize);
        for (int i = 1; i < pieceCount; i++) {
          e2 = new Entry(e.path, e);
          e2.len = pieceSize;
          e2.off = offset;
          offset += pieceSize + 1;
          nl.addEntry(e2);
        }
        e2 = new Entry(e.path, e);
        e2.len = -1;
        e2.off = offset;
        nl.addEntry(e2);
        //iter.remove();
      } else {
        e2 = new Entry(e.path, e);
        nl.addEntry(e2);
      }
    }
    return nl;
  }

  public double avgFileSize() {
    return count == 0 ? 0 : size / count;
  }

  public Iterator<Entry> iterator() {
    return list.iterator();
  }

  public void updateDestinationPaths() {
    for (Entry e : list) {
      if (dp.compareTo("/dev/null") == 0) {
        e.setdpath(dp);
      } else {
        e.setdpath(dp + e.path);
      }
    }
  }

  public void shuffleList() {
    Collections.shuffle(list);
  }

  // An entry (file or directory) in the list.
  public class Entry {
    public final boolean dir;
    public final long size;
    public String path, dpath;
    public boolean done = false;
    public long off = 0, len = -1;  // Start/stop offsets.

    // Create a directory entry.
    Entry(String path) {
      if (path == null) {
        path = "";
      } else if (!path.endsWith("/")) {
        path += "/";
      }
      path = path.replaceFirst("^/+", "");
      this.path = path;
      size = off = 0;
      dir = true;
    }

    // Create a file entry.
    public Entry(String path, long size) {
      this.path = path;
      this.size = (size < 0) ? 0 : size;
      off = 0;
      dir = false;
    }

    // Create an entry based off another entry with a new path.
    Entry(String path, Entry e) {
      this.path = path;
      dir = e.dir;
      done = e.done;
      size = e.size;
      off = e.off;
      len = e.len;
    }

    public long remaining() {
      if (done || dir) {
        return 0;
      }
      if (off > size) {
        return 0;
      }
      //if (len == -1 || len > size)
      return size - off;
      //return len - off;
    }

    public long size() {
      if (done || dir) {
        return 0;
      }
      if (off > size) {
        return 0;
      }
      if (len == -1) {
        return size - off;
      }
      return len;
    }

    public String path() {
      return XferList.this.sp + path;
    }

    public void setdpath(String dp) {
      dpath = dp;
    }

    public String dpath() {
      //return XferList.this.dp + path;
      return dpath;
    }

    public String toString() {
      return (dir ? "Directory: " : "File: ") + path() + " -> " + dpath();
    }
  }
}