package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek((long) pid.getPageNumber() * BufferPool.getPageSize());

            byte[] buffer = new byte[BufferPool.getPageSize()];
            raf.read(buffer);
            raf.close();
            return new HeapPage((HeapPageId) pid, buffer);
        } catch (Exception e) {
            throw new IllegalArgumentException("HeapFile.readPage error");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        byte[] data = page.getPageData();
        if (pgNo < numPages()) {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek((long) pgNo * BufferPool.getPageSize());
            raf.write(data);
            raf.close();
        } else {
            FileOutputStream fos = new FileOutputStream(file, true);
            fos.write(data);
            fos.close();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(file.length() / (double) BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        for (int i = 0; i < numPages(); i++) {
            try {
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
                page.insertTuple(t);
                page.markDirty(true, tid);
                return new ArrayList<>(Collections.singletonList(page));
            } catch (DbException de) {
                if (de.getMessage().equals("HeapPage.insertTuple(): tupledesc mismatch")) {
                    throw de;
                }
            }
        }
        // should get here when all pages are full
        int i = numPages();
        HeapPage newPage = allocatePage(tid, i);
        newPage.insertTuple(t);
        return new ArrayList<>(Collections.singletonList(newPage));
    }

    // 1. append a page of bytes to the physical file
    // 2. return a new HeapPage object representing that page of bytes.
    private HeapPage allocatePage(TransactionId tid, int pgNo) throws FileNotFoundException, IOException, TransactionAbortedException, DbException {
        FileOutputStream fos = new FileOutputStream(file, true);
        fos.write(new byte[BufferPool.getPageSize()]);
        fos.close();
        HeapPageId hPid = new HeapPageId(getId(), pgNo);
        // check that new bytes are indeed written to the physical file.
        return (HeapPage) Database.getBufferPool().getPage(tid, hPid, Permissions.READ_WRITE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != getId()) {
            throw new DbException("HeapFile.deleteTuple()");
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
        // not necessary for lab1
    }

    public class HeapFileIterator implements DbFileIterator {
        Iterator<Tuple> it = null;
        TransactionId tid;
        HeapFile heapFile;
        boolean opened = false;
        int pgNo = 0;


        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            opened = true;
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(heapFile.getId(), pgNo), Permissions.READ_ONLY);
            it = heapPage.iterator();

            nextHelper(true);

            if (!hasNext()) {
                close();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return opened && pgNo < numPages() && it.hasNext();
        }

        private Tuple nextHelper(boolean calledInOpen) throws TransactionAbortedException, DbException {
            Tuple res = calledInOpen ? null : it.next();
            while (!it.hasNext() && pgNo < numPages()) {
                it = null;
                pgNo += 1;
                // 这个时候it必须有next。
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(heapFile.getId(), pgNo), Permissions.READ_ONLY);
                it = heapPage.iterator();
            }
            return res;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return nextHelper(false);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pgNo = 0;
            open();
        }

        @Override
        public void close() {
            opened = false;
        }
    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}

