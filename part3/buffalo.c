/*
Authors:
Christian La Bri, 9080638605
Yash Butani, 9080409809
Dylan Kuncheria, 9082113425

This file provides the buffer functionality of the buffer system class BufMgr 
in the Minirel database system.
*/

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/**
 * This function allocates and returns a free frame in the buffer.
 *
 * @param frame Frame number reference. The frame number that is freed is returned through this reference.
 * 
 * @returns Status BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an
 * error when a dirty page was being written to disk, and OK otherwise.
 */
const Status BufMgr::allocBuf(int & frame) {
bool is_allocated = false;
    int count = 0;
    Status status;

while (count < numBufs * 2) {
    BufDesc* desc = &bufTable[clockHand];

    if (desc->valid == true) {
        if (desc->refbit == true) {
            //clear ref bit
            desc->refbit = false;
            advanceClock();
            count++;
        }
        else {
            if (desc->pinCnt == 0) {
                if (desc->dirty == true) {
                    //flush page to disk
                    status = desc->file->writePage(desc->pageNo, &bufPool[clockHand]);
                    if (status != OK) {
                        return UNIXERR;
                    }
                    bufStats.accesses++;
                    status = hashTable->remove(desc->file, desc->pageNo);
                    if (status != OK ) {
                        return status;
                    }
                    desc->Clear();
                    frame = clockHand;
                    bufStats.diskwrites++;
                    is_allocated = true;
                    break;
                }
                else { //just clear the frame
                    status = hashTable->remove(desc->file, desc->pageNo);
                    if (status != OK ) {
                        return status;
                    }
                    is_allocated = true;
                    frame = clockHand;
                    break;
                }
            }
            else {
                advanceClock(); //page has been pinned so advance clock
                count++;
            }
        }
    }
    else {
        //if not a valid page, just overwrite
        is_allocated = true;
        frame = clockHand;
        break;
    }
    }
    if (is_allocated == false) { //could not find an open frame
       return BUFFEREXCEEDED;
    }
    return OK;
}

/**
 * Reads the page from the file into a bufferframe and returns the pointer to page.
 * If the page is already present in the buffer pool, then the pointer to that frame is returned.
 * If the page is not present, a new frame is allocated for reading the page.
 *
 * @param file   	File object.
 * @param PageNo    Page number to be read.
 * @param page  	Reference to page. The reference is returned via this variable.
 * 
 * @returns Status OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer
 * frames are pinned, HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    //case1: page is not in buffer pool
    if (status == HASHNOTFOUND) {
        status = allocBuf(frameNo); //allocate frame
        if (status != OK)
        {
            return status;
        }
        status = file->readPage(PageNo, &bufPool[frameNo]); //read page in
        if (status != OK)
        {
            return status;
        }
        status = hashTable->insert(file, PageNo, frameNo); //insert page into hashtable
        if (status != OK)
        {
            return status;
        }
        bufTable[frameNo].Set(file, PageNo);
        page = &bufPool[frameNo];
        return OK;
    }
    //case2: page is in buffer pool
    else {
        BufDesc* desc = &bufTable[frameNo];
        desc->refbit = true;
        desc->pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }

    return OK;
}

/**
 * Unpins a page after a process is done using it.
 *
 * @param file    File object
 * @param PageNo  Page number
 * @param dirty   True if the page to be unpinned needs to be marked dirty.
 * 
 * @returns  OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table,
 * PAGENOTPINNED if the pin count is already 0.
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) {

    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status != OK) {
        return status;
    }

    if (bufTable[frameNo].pinCnt == 0) {
        return PAGENOTPINNED;
    }

    bufTable[frameNo].pinCnt--;

    if (dirty == true) { 
        bufTable[frameNo].dirty = true;
    }

    return OK;
}

/**
 * Allocates a new page in the file for use by a process.
 * The page is also given a slot in the buffer pool/table.
 *
 * @param file   	File object
 * @param PageNo    Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page. The reference is returned via this variable.
 * 
 * @returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames 
 * are pinned and HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {

    Page newPage;
    Status status = file->allocatePage(pageNo);
    if (status != OK) {
        return status;
    }
    int frameNo;
    status = allocBuf(frameNo); //allocate a buffer frame for the new page
    if (status != OK) {
        return status;
    }
    bufPool[frameNo] = newPage;
    bufStats.accesses++;
    page = &bufPool[frameNo];

    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK) {
        return status;
    }
    bufTable[frameNo].Set(file, pageNo);
        return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
/*
Authors:
Christian La Bri, 9080638605
Yash Butani, 9080409809
Dylan Kuncheria, 9082113425

This file provides the buffer functionality of the buffer system class BufMgr 
in the Minirel database system.
*/

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/**
 * This function allocates and returns a free frame in the buffer.
 *
 * @param frame Frame number reference. The frame number that is freed is returned through this reference.
 * 
 * @returns BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an
 * error when a dirty page was being written to disk, and OK otherwise.
 */
const Status BufMgr::allocBuf(int & frame) {
bool is_allocated = false;
    int count = 0;
    Status status;

while (count < numBufs * 2) {
    BufDesc* desc = &bufTable[clockHand];

    if (desc->valid == true) {
        if (desc->refbit == true) {
            //clear ref bit
            desc->refbit = false;
            advanceClock();
            count++;
        }
        else {
            if (desc->pinCnt == 0) {
                if (desc->dirty == true) {
                    //flush page to disk
                    status = desc->file->writePage(desc->pageNo, &bufPool[clockHand]);
                    if (status != OK) {
                        return UNIXERR;
                    }
                    bufStats.accesses++;
                    status = hashTable->remove(desc->file, desc->pageNo);
                    if (status != OK ) {
                        return status;
                    }
                    desc->Clear();
                    frame = clockHand;
                    bufStats.diskwrites++;
                    is_allocated = true;
                    break;
                }
                else { //just clear the frame
                    status = hashTable->remove(desc->file, desc->pageNo);
                    if (status != OK ) {
                        return status;
                    }
                    is_allocated = true;
                    frame = clockHand;
                    break;
                }
            }
            else {
                advanceClock(); //page has been pinned so advance clock
                count++;
            }
        }
    }
    else {
        //invoke set on frame
        is_allocated = true;
        frame = clockHand;
        break;
    }
    }
    if (is_allocated == false) { //could not find an open frame
       return BUFFEREXCEEDED;
    }
    return OK;
}

/**
 * Reads the page from the file into a frame and returns the pointer to page.
 * If the page is already present in the buffer pool, then the pointer to that frame is returned.
 * If the page is not present, a new frame is allocated for reading the page.
 *
 * @param file   	File object.
 * @param PageNo    Page number to be read.
 * @param page  	Reference to page. The reference is returned via this variable.
 * 
 * @returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer
 * frames are pinned, HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    //case1: page is not in buffer pool
    if (status == HASHNOTFOUND) {
        status = allocBuf(frameNo); //allocate frame
        if (status != OK)
        {
            return status;
        }
        status = file->readPage(PageNo, &bufPool[frameNo]); //read page in
        if (status != OK)
        {
            return status;
        }
        status = hashTable->insert(file, PageNo, frameNo); //insert page into hashtable
        if (status != OK)
        {
            return status;
        }
        bufTable[frameNo].Set(file, PageNo);
        page = &bufPool[frameNo];
        return OK;
    }
    //case2: page is in buffer pool
    else {
        BufDesc* desc = &bufTable[frameNo];
        desc->refbit = true;
        desc->pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }

    return OK;
}

/**
 * Unpins a page after a process is done using it.
 *
 * @param file    File object
 * @param PageNo  Page number
 * @param dirty   True if the page to be unpinned needs to be marked dirty
 * 
 * @returns  OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table,
 * PAGENOTPINNED if the pin count is already 0.
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) {

    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status != OK) {
        return status;
    }

    if (bufTable[frameNo].pinCnt == 0) {
        return PAGENOTPINNED;
    }

    bufTable[frameNo].pinCnt--;

    if (dirty == true) { 
        bufTable[frameNo].dirty = true;
    }

    return OK;
}

/**
 * Allocates a new page in the file for use by a process.
 * The page is also given a slot in the buffer pool/table.
 *
 * @param file   	File object
 * @param PageNo    Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page. The reference is returned via this variable.
 * 
 * @returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames 
 * are pinned and HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {

    Page newPage;
    Status status = file->allocatePage(pageNo);
    if (status != OK) {
        return status;
    }
    int frameNo;
    status = allocBuf(frameNo); //allocate a buffer frame for the new page
    if (status != OK) {
        return status;
    }
    bufPool[frameNo] = newPage;
    bufStats.accesses++;
    page = &bufPool[frameNo];

    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK) {
        return status;
    }
    bufTable[frameNo].Set(file, pageNo);
        return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}



