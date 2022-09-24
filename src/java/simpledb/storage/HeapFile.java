package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td)  {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid)  {
        // some code goes here
        int len = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * len;
        byte[] page = new byte[len];

        int count = -1;
        try {
            RandomAccessFile raf =new RandomAccessFile(this.getFile(), "r");
            raf.seek(offset);
            count = raf.read(page, 0, len);
            if (count == -1) {
                throw new IOException("pid invalid");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            return new HeapPage((HeapPageId) pid, page);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int len = BufferPool.getPageSize();
        int offset = page.getId().getPageNumber() * len;
        byte[] data = page.getPageData();

        RandomAccessFile raf =new RandomAccessFile(this.getFile(), "rw");
        raf.seek(offset);
        raf.write(data);
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor ((double) this.f.length() / (double) BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        for (int i = 0; i < numPages(); i++) {
            HeapPage hp = (HeapPage)Database.getBufferPool().getPage(
                tid, new HeapPageId(getId(), i), null);
            if (hp.getNumEmptySlots() != 0) {
                hp.insertTuple(t);
                writePage(hp);
                return new ArrayList<>(Collections.singletonList(hp));
            }
        }
        // add new page
        HeapPage hp = new HeapPage(
            new HeapPageId(getId(), numPages()),
            HeapPage.createEmptyPageData());
        hp.insertTuple(t);
        writePage(hp);

        return new ArrayList<>(Collections.singletonList(hp));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId rid = t.getRecordId();

        HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, rid.getPageId(), null);
        hp.deleteTuple(t);
        try {
            writePage(hp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        hp.markDirty(true, tid);

        return new ArrayList<>(Collections.singletonList(hp));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            int index = 0;
            Iterator<Tuple> curIt = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (index < numPages()) {
                    PageId pid = new HeapPageId(getId(), this.index);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
                    curIt = page.iterator();
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (this.curIt == null) {
                    return false;
                }
                while (true) {
                    if (this.curIt.hasNext()) {
                        return true;
                    }
                    if (++index >= numPages()) {
                        break;
                    }
                    PageId pid = new HeapPageId(getId(), this.index);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
                    curIt = page.iterator();
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (this.curIt == null) {
                    throw new NoSuchElementException("not open");
                }
                return this.curIt.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                index = 0;
                if (index < numPages()) {
                    PageId pid = new HeapPageId(getId(), this.index);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
                    curIt = page.iterator();
                }
            }

            @Override
            public void close() {
                this.index = 0;
                this.curIt = null;
            }
        };
    }
}

