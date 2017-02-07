/**
 * @author TARUN BANSAL, SIMMI PATERIYA, SRIPRADHA KARKALA
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/*
 Write all the modified pages to disk and free the memory
 allocated for buf description and the hashtable.
*/
BufMgr::~BufMgr()
{
  // Write all the dirty pages in the bufpool to disc

  for(FrameId i =0; i<numBufs;i++){
    if(bufDescTable[i].dirty == true)
      bufDescTable[i].file->writePage(bufPool[i]);
    }
  free(bufDescTable);
  delete[] bufPool;
  free(hashTable);
}

/*
 Increment the clockhand within the circular buffer pool .
*/
void BufMgr::advanceClock()
{
  clockHand = (clockHand+1)%numBufs;
}

/*
 This function allocates a new frame in the buffer pool
 for the page to be read. The method used to allocate
 a new frame is the clock algorithm.
*/
void BufMgr::allocBuf(FrameId & frame) 
{
  std::uint32_t pcount = 0;

  while(1){ // Frame has not been allocated yet
    advanceClock();

    //All the frames in the buffer pool are pinned to be used
    if(pcount == numBufs){
      throw BufferExceededException();
    }
    //Use the frame from the buffer pool if it is not valid
    if(bufDescTable[clockHand].valid == false){
      frame = clockHand;
      bufDescTable[clockHand].Clear();
      break;
    }
    //Check the pincount of the frame before using it
    else if(bufDescTable[clockHand].pinCnt >0){
      pcount=pcount+1;
      continue;
    }
    //If recently referenced, set the refbit to false and move on
    else if(bufDescTable[clockHand].refbit == true){
      bufDescTable[clockHand].refbit = false;
      continue;
    }
    // Write to disk if the page is dirty and then use the frame
    else if(bufDescTable[clockHand].dirty == true){
      bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
    }
    hashTable->remove(bufDescTable[clockHand].file,
		      bufDescTable[clockHand].pageNo);
    bufDescTable[clockHand].Clear();
    frame = clockHand;
    break;
 }
}

/*
 This function reads a page of a file from the buffer pool
 if it exists. Else, fetches the page from disk, allocates
 a frame in the bufpool by calling allocBuf function and
 returns the Page.
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId i;
  try{
    // Page exists in the buffer pool
    hashTable->lookup(file, pageNo, i);
    bufDescTable[i].refbit = true;
    bufDescTable[i].pinCnt++;
    page = &bufPool[i];
  }
  catch(HashNotFoundException hnf){
    // Page doesnt exist in the buffer pool, read from disk and allocate a frame in the buffer pool
    allocBuf(i);
    bufPool[i] = file->readPage(pageNo);
    hashTable->insert(file, pageNo, i);
    bufDescTable[i].Set(file, pageNo);
    page = &bufPool[i];
 }
}

/*
 This function decrements the pincount for a page from the buffer pool.
 Checks if the page is modified, then sets the dirty bit to true.
 If the page is already unpinned throws a PageNotPinned exception.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  try{
    FrameId f ;
    hashTable->lookup(file,pageNo,f);
    BufDesc* tmpbuf = &(bufDescTable[f]);
    // Page already unpinned
    if(tmpbuf->pinCnt == 0) {
      throw PageNotPinnedException(file->filename(),pageNo,f);
    }
    // Check if dirty,then reduce the pincount
    else if (tmpbuf->pinCnt > 0) {
      if (dirty)
        bufDescTable[f].dirty = true;
    	tmpbuf->pinCnt--;
      }
  }
  catch(HashNotFoundException hnf){
    // The page was not found in the buffer pool.
  }
}

/*
 Checks for all the pages which belong to the file in the buffer pool.
 If the page is modified, then writes the file to disk and clears it
 from the Buffer manager. Else, if its being referenced by other
 services, then throws a pagePinnedException.
 Else if the frame is not valid then throws a BadBufferException.
*/
void BufMgr::flushFile(const File* file) 
{
  for(FrameId i = 0 ;i<numBufs; i++) {
    if(bufDescTable[i].file == file) { // For the given file
      if(bufDescTable[i].valid == false) {
        throw BadBufferException(i, bufDescTable[i].dirty,
			        bufDescTable[i].valid, bufDescTable[i].refbit);
      }
      // Page in use, then throw PagePinnedexception
      else if(bufDescTable[i].pinCnt > 0) {
        throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
      }
      // If page is dirty, write it to disk and then flush the page
      else if(bufDescTable[i].dirty == true) {
      	Page page = bufPool[bufDescTable[i].frameNo];
      	bufDescTable[i].file->writePage(page);
      	bufDescTable[i].dirty = false;
      	hashTable->remove(file, bufDescTable[i].pageNo);
      	bufDescTable[i].Clear();
      }
    }
  }
}

/*
 This function allocates a new page and reads it into the buffer pool.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  Page p = file->allocatePage();
  FrameId f;
  allocBuf(f);
  bufPool[f] = p;
  hashTable->insert(file, p.page_number(), f);
  bufDescTable[f].Set(file, p.page_number());
  pageNo = p.page_number();
  page = &bufPool[f];
}

/* This function is used for disposing a page from the buffer pool
   And delete it from the corresponding file
 */

void BufMgr::disposePage(File* file, const PageId PageNo)
{
  FrameId fid;
  try {
    hashTable->lookup(file, PageNo, fid);
    bufDescTable[fid].Clear();
    hashTable->remove(file, PageNo);
    file->deletePage(PageNo);
  }
  catch (HashNotFoundException hnf) {
    std::cout << "Failed with exception : "<< hnf.what();
  }
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
