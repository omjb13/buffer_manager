/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
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


BufMgr::~BufMgr() {
  for(std::uint32_t i = 0; i < numBufs; i++) {
    // Flush out all dirty pages
    if(bufDescTable[i].dirty) {
      flushFile(bufDescTable[i].file);
    }
  }
  
  // deallocate BufDescTable
  delete [] bufDescTable;

  // deallocate buffer pool
  delete [] bufPool;
}

void BufMgr::advanceClock()
{
  // Advance clock hand to the next frame
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame)
{
  bool unpinned = false;
  for(std::uint32_t i = 0; i < numBufs; i++) {
    // See if we have at least one unpinned page
    if(bufDescTable[i].pinCnt == 0) {
      unpinned = true;
      break;
    }
  }

  // Throw exception if all buffer frames are pinned
  if(!unpinned) {
    throw BufferExceededException();
  }

  // Clock Algorithm
  while(true) {
    advanceClock();
    uint32_t index = clockHand;
    if(bufDescTable[index].valid) {
      if(bufDescTable[index].refbit) {
	// Clear reference bit and continue
	bufDescTable[index].refbit = false;
	continue;
      }
      if(bufDescTable[index].pinCnt>=1) {
	// Pinned page, so continue
	continue;
      }
      if(bufDescTable[index].dirty) {
	// Flush the dirty pages
	flushFile(bufDescTable[index].file);
      }
      // Found a valid frame
      frame = bufDescTable[index].frameNo;
      return;
    } else {
      // Found an invalid frame
      frame = bufDescTable[index].frameNo;
      return;
    }
  }
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId currentFrameId;
  try {
    hashTable->lookup(file, pageNo, currentFrameId);
    // The page is already in the buffer pool
    bufDescTable[currentFrameId].refbit = true;
    bufDescTable[currentFrameId].pinCnt += 1;
    page = &bufPool[currentFrameId];
  } catch(HashNotFoundException) {
    // The page is not in the buffer pool, so allocate a new frame
    FrameId newFrameId;
    allocBuf(newFrameId);
    if(bufDescTable[newFrameId].valid) {
      try {
	// Update the hastable if we use a valid frame
       	hashTable->remove(bufDescTable[newFrameId].file, bufDescTable[newFrameId].pageNo);
      } catch(HashNotFoundException) {
      }
    }
    bufPool[newFrameId] = file->readPage(pageNo);
    hashTable->insert(file, pageNo, newFrameId);
    bufDescTable[newFrameId].Set(file, pageNo);
    page = &bufPool[newFrameId];
  }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId currentFrameId;
  try {
    hashTable->lookup(file, pageNo, currentFrameId);
    if(bufDescTable[currentFrameId].pinCnt == 0 ) {
      // Thorw exception if page is not pinned
      throw PageNotPinnedException(file->filename(), pageNo, currentFrameId);
    }
    bufDescTable[currentFrameId].pinCnt -= 1;
    if(dirty) {
      bufDescTable[currentFrameId].dirty = true;
    }
  } catch(HashNotFoundException) {
  }
}

void BufMgr::flushFile(const File* file) 
{
  for(std::uint32_t i = 0; i < numBufs; i++) {
    if(bufDescTable[i].file == file) {
      // Throw exception if some page is pinned
      if(bufDescTable[i].pinCnt >= 1) {
  	throw PagePinnedException(file->filename(), bufDescTable[i].pageNo,
  				  bufDescTable[i].frameNo);
      }
      if(bufDescTable[i].pageNo == 0) {
	// Throw excpetion if some invalid page is being referenced
  	throw BadBufferException(bufDescTable[i].frameNo,
  				 bufDescTable[i].dirty,
  				 bufDescTable[i].valid,
  				 bufDescTable[i].refbit);
      }
    }
  }  
  for(std::uint32_t i = 0; i < numBufs; i++) {
    if(bufDescTable[i].file == file) {
      if(bufDescTable[i].dirty) {
	// Write back any dirty pages
	bufDescTable[i].file->writePage(bufPool[i]);
	bufDescTable[i].dirty = false;
      }
      // Update the hastable and clear the buff desc table
      hashTable->remove(file, bufDescTable[i].pageNo);
      bufDescTable[i].Clear();
    }
  }  
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  Page newPage = file->allocatePage();
  pageNo = newPage.page_number();
  FrameId newFrameId;
  allocBuf(newFrameId);
  if(bufDescTable[newFrameId].valid) {
    try {
      // Update the hastable if we use a valid frame
      hashTable->remove(bufDescTable[newFrameId].file, bufDescTable[newFrameId].pageNo);
    } catch(HashNotFoundException) {
    }
  }
  // Update the hastable to insert new page and set the buff desc table
  hashTable->insert(file, pageNo, newFrameId);
  bufDescTable[newFrameId].Set(file, pageNo);
  bufPool[newFrameId] = newPage;
  page = &bufPool[newFrameId];
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
  FrameId currentFrameId;
  try {
    hashTable->lookup(file, PageNo, currentFrameId);
    bufDescTable[currentFrameId].Clear();
    hashTable->remove(file, bufDescTable[currentFrameId].pageNo);
  } catch(HashNotFoundException) {
  }  
  file->deletePage(PageNo);
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
