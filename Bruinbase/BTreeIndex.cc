/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid    = -1;
    treeHeight =  0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    RC openRes = pf.open(indexname,mode);
    if(openRes == 0){
        char temp[PafeFile::PAGE_SIZE];
        RC readRes = pf.read(META_PID, temp);
        if(readRes == 0){
            memcpy(&rootPid, temp, sizeof(rootPid));
            memcpy(&treeHeight, temp + sizeof(rootPid), sizeof(treeHeight));
        } else{
            return readRes;
        }
    }
    return openRes;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    char temp[PageFile::PAGE_SIZE];
    memcpy(temp, &rootPid, sizeof(rootPid));
    memcpy(temp + sizeof(rootPid), &treeHeight, sizeof(treeHeight));
    RC writeRes = pf.write(META_PID,temp);
    if(writeRes != 0) return writeRes;
    return pf.close();
}

RC BTreeIndex::insertRecursive(int             key,
                               const RecordId& rid,
                               PageId          pid,
                               int             currentHeight,
                               int&            outKey
                               PageId&         outPid)
{
    RC result;
    if(currentHeight > 1) {
        BTNonLeafNode node;
        result = node.read(pid, pf);
        if(result != 0) {
            return result;
        }

        PageId childPid;

        result = node.locateChildPtr(key, childPid);
        if(result != 0) {
            // This should never happen
            return result;
        }

        int    possibleKey = -1;
        PageId possiblePid = -1;
        result = insertRecursive(key,
                                 rid,
                                 childPid,
                                 currentHeight-1,
                                 possibleKey,
                                 possiblePid);
        if(result != 0) {
            return result;
        }

        if(possibleKey != -1 && possiblePid != -1) {
            result = node.insert(possibleKey, possiblePid);
            if(result != 0) {
                BTNonLeafNode sibling;
                int midKey;
                result = leaf.insertAndSplit(possibleKey, possiblePid, sibling, midKey);
                if(result != 0) {
                    return result;
                }

                PageId siblingPid = pf.endPid();

                result = sibling.write(siblingPid, pf);
                if(result != 0) {
                    return result;
                }

                outKey = midKey;
                outPid = siblingPid;
            }

            return node.write(pid, pf);
        }
    } else if(currentHeight == 1) {
        BTLeafNode leaf;
        result = leaf.read(pid, pf);
        if(result != 0) {
            return result;
        }

        result = leaf.insert(key, rid);
        if(result != 0) {
            BTLeafNode sibling;
            int siblingKey;
            // Could not fit, we need to call insertAndSplit.
            result = leaf.insertAndSplit(key, rid, sibling, siblingKey);
            if(result != 0) {
                return result;
            }


            PageId siblingPid = pf.endPid();

            PageId oldPointed = leaf.getNextNodePtr();
            result = sibling.setNextNodePtr(oldPointed);
            if(result != 0) {
                return result;
            }


            result = leaf.setNextNodePtr(siblingPid);
            if(result != 0) {
                return result;
            }

            result = sibling.write(siblingPid, pf);
            if(result != 0) {
                return result;
            }

            outKey = siblingKey;
            outPid = siblingPid;
        }

        return leaf.write(pid, pf);
    }

    return -1;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    RC result;
    if(treeHeight == 0) {
        // Must create a leaf node
        BTLeafNode root;
        result = root.insert(key, rid);
        if(result != 0) {
            // Should never happen, but better safe than sorry!
            return result;
        }
        rootPid = pf.endPid(); // Should be 1
        result = root.write(pid, pf);
        if(result != 0) return result;
        treeHeight++;
    } else {
        int    possibleKey = -1;
        PageId possiblePid = -1;
        result = insertRecursive(key,
                                 rid,
                                 rootPid,
                                 treeHeight,
                                 possibleKey,
                                 possiblePid);
        if(result != 0) return result;

        if(possibleKey != -1 && possiblePid != -1) {
            BTNonLeafNode newRoot;
            result = newRoot.initializeRoot(rootPid, possibleKey, possiblePid);
            if(result != 0) return result;

            PageId newRootPid = pf.endPid();

            result = newRoot.write(newRootPid, pf);
            if(result != 0) return result;

            rootPid = newRootPid;

            treeHeight++;
        }
    }

    return 0;
}

/*
 * Find the leaf-node index entry whose key value is larger than or
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return error code. 0 if no error.
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
    if(treeHeight == 0) {
        return -1;
    }

    int currentPid = rootPid;
    int height;
    for(height = treeHeight; height > 1; height--) {
        BTNonLeafNode node;
        int result = node.read(currentPid, pf);
        if(result != 0) {
            return result;
        }

        result = node.locateChildPtr(searchKey, currentPid);
        if(result != 0) {
            // This should never happen
            return result;
        }
    }

    BTLeafNode node;
    int result = node.read(currentPid, pf);
    if(result != 0) {
        return result;
    }

    result = node.locate(searchKey, cursor.eid);
    if(result != 0) {
        return result;
    }
    cursor.pid = currentPid;

    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    // char temp[PageFile::PAGE_SIZE];
    // RC readRes = pf.read(cursor.pid, temp);
    BTLeafNode leaf;
    RC readRes = leaf.read(cursor.pid, pf);
    if(readRes != 0) return readRes;
    RC readEntryRes = leaf.readEntry(cursor.eid, key, rid);
    return readEntryRes;
}
