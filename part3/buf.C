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

/*
Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk. Returns
BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an
error when a dirty page was being written to disk, and OK otherwise. This private method will get called
by the readPage() and allocPage() methods described below.
Make sure that if the buffer frame allocated has a valid page in it, you remove the appropriate entry from
the hash table.
*/
const Status BufMgr::allocBuf(int & frame) 
{
bool is_allocated = false;
    int count = 0;
    Status status;
    //printf("here");

while(count < numBufs * 2)
    {
        //printf("Here");
    if(bufTable[clockHand].valid == true)
    {
        if(bufTable[clockHand].refbit == true)
        {
            //clear ref bit
            bufTable[clockHand].refbit = false;
            advanceClock();
            count++;
        }
        else
        {
            if(bufTable[clockHand].pinCnt == 0)
            {
                if(bufTable[clockHand].dirty == true)
                {
                    //flush page to disk
                    status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo,&bufPool[clockHand]);
                    if(status != OK)
                    {
                        return UNIXERR;
                    }
                    bufStats.accesses++;
                    status = hashTable->remove(bufTable[clockHand].file,bufTable[clockHand].pageNo);
                    if(status != OK )
                    {
                        return status;
                    }
                    bufTable[clockHand].Clear();
                    frame = clockHand;
                    bufStats.diskwrites++;
                    is_allocated= true;
                    break;
                }
                else {
                    is_allocated = true;
                    frame = clockHand;
                    break;
                }
            }
            else
            {
                advanceClock(); //page has been pinned so advance clock
                count++;
            }
        }
    }
    else
    {
        //invoke set on frame
        is_allocated = true;
        frame = clockHand;
        break;
    }
    }
    if(is_allocated == false)
    {
       return BUFFEREXCEEDED;
    }
    return OK;
}

/*
    First check whether the page is already in the buffer pool by invoking the lookup() method on the
hashtable to get a frame number. There are two cases to be handled depending on the outcome of the
lookup() call:
    Case 1. The page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and then call the
method file->readPage() to read the page from the disk into the buffer pool frame. Next, insert the page
into the hashtable. Finally, invoke Set() on the frame to set it up properly. Set() will leave the pinCnt for
the page set to 1. Return a pointer to the frame containing the page via the page parameter.
    Case 2. The page is in the buffer pool. In this case set the appropriate refbit, increment the pinCnt for the
page, and then return a pointer to the frame containing the page via the page parameter.
Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer
frames are pinned, HASHTBLERROR if a hash table error occurred.
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    //case1: page is not in buffer pool
    if (status == HASHNOTFOUND) {
        status = allocBuf(frameNo);
        if (status != OK)
        {
            return status;
        }
        status = file->readPage(PageNo, &bufPool[frameNo]); //check to see if explicit page set is needed
        if (status != OK)
        {
            return status;
        }
        //May need to change to manual insertion, double check
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK)
        {
            return status;
        }
        bufTable[frameNo].Set(file, PageNo);  //CHANGE?
        page = &bufPool[frameNo];
        return OK; //pointer to frame containing page via page parameter //EDITTTTTTTTT
    }
    //case2: page is in buffer pool
    else {
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
        return OK; //pointer to frame containing page via page parameter //EDITTTTTTTTT
    }

    return OK;
}

/*
Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true sets the dirty bit.
Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table,
PAGENOTPINNED if the pin count is already 0.
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{

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

/*
This call is kind of weird. The first step is to allocate an empty page in the specified file by invoking the
file->allocatePage() method. This method will return the page number of the newly allocated page. Then
allocBuf() is called to obtain a buffer pool frame. Next, an entry is inserted into the hash table and Set() is
invoked on the frame to set it up properly. The method returns both the page number of the newly
allocated page to the caller via the pageNo parameter and a pointer to the buffer frame allocated for the
page via the page parameter. Returns OK if no errors occurred, UNIXERR if a Unix error occurred,
BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error
occurred.
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
//possibly make frameID unsigned
Page newPage;
Status status = file->allocatePage(pageNo);
if (status != OK) {
    return status;
}
int frameNo;
status = allocBuf(frameNo);
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


