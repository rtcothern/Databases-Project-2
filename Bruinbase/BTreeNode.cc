#include "BTreeNode.h"

using namespace std;

BTLeafNode::BTLeafNode()
{
    // Ensure that we are always in a valid state
    // We are using '-1' for an invalid page
    buff.nodeData.keyCount = 0;
    buff.nodeData.nextNode = -1;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{

    int status = pf.read(pid, buff.raw_buff);
    return status;
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
    int status = pf.write(pid, buff.raw_buff);
    return status;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
    return buff.nodeData.keyCount;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
    if(getKeyCount() >= MAX_ENTRIES) {
        // The contract for this function specifies that we must return
        // an error code if the node is full:
        return -1;
    } else {
        const int numKeys = getKeyCount();

        int eid;
        int status = locate(key, eid);
        if(status == 0) {
            // Locate succeeded.
            // Loop from the first non-used key to the
            // key right after 'eid'. Pull everything from
            // the left forward.
            for(int i = numKeys; i > eid; i--) {
                buff.nodeData.entries[i] = buff.nodeData.entries[i-1];
            }
        } else {
            eid = numKeys;
        }

        // Actually place the value
        BuffEntry temp = {rid, key};
        buff.nodeData.entries[eid] = temp;

        buff.nodeData.keyCount++;

        return 0;
    }
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid,
                              BTLeafNode& sibling, int& siblingKey)
{
    // Just some error checking
    if(getKeyCount() != MAX_ENTRIES) {
        return -1;
    }

    const int half = MAX_ENTRIES / 2;
    siblingKey = buff.nodeData.entries[half].key;

    // Before we do any work, we can update the keyCount
    buff.nodeData.keyCount = half;

    // Because we do not have a 'pid' for the sibling, we
    // must wait for our caller to set our pid to the pid of the
    // sibling.
    // TODO: Make sure BTreeIndex does this when calling us!
    sibling.buff.nodeData.nextNode = buff.nodeData.nextNode;

    if(key >= siblingKey) {
        // We must insert the key into the sibling.
        // Use a loop to copy instead of memcpy, so that
        // we can save time on an insert by placing it where
        // necessary.
        bool found = false;
        int i = half, j = 0;
        for(; i < MAX_ENTRIES; i++, j++) {
            if(!found && buff.nodeData.entries[i].key >= key) {
                // We have finally found the spot we need
                // Insert the new key right here
                BuffEntry temp = {rid, key};
                sibling.buff.nodeData.entries[j] = temp;

                // Increment j and let the process continue on
                j++;

                // Flag us for the future
                found = true;
            } else {
                // Just do a normal data copy
                sibling.buff.nodeData.entries[j] = buff.nodeData.entries[i];
            }
        }

        if(!found) {
            // We need to insert the entry at the end:
            BuffEntry temp = {rid, key};
            sibling.buff.nodeData.entries[j] = temp;
        }

        // Now we must set the appropriate keyCount
        // We must include an addition of 1 for the new entry
        sibling.buff.nodeData.keyCount = MAX_ENTRIES - half + 1;

        // Note that insertion of the item to the sibling
        // will NOT change the returned siblingKey, because
        // if it were inserted before the first entry, then
        // it would have been inserted in the left node!
    } else {
        // We should not insert it in the sibling. We
        // should insert it here and do a memcpy
        memcpy(sibling.buff.nodeData.entries, buff.nodeData.entries+half, MAX_ENTRIES-half);
        sibling.buff.nodeData.keyCount = MAX_ENTRIES - half;

        // Now we can just call our insert routine to insert
        // the proper values. Remember that keyCount was fixed above,
        // so insert knows what to do
        int status = insert(key, rid);
        if(status != 0) return status;
    }

    return 0;
}

/*
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the entry number that contains a key larger than or equalty to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
    //TODO: Use binary search instead of linear
    const int numKeys = getKeyCount();
    for(int i = 0; i < numKeys; i++){
        if(buff.nodeData.entries[i].key >= searchKey) {
            eid = i;
            return 0;
        }
    }
    // Key with value larger than or equal to
    // searchKey was not found
    return -1;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
    if(eid < getKeyCount()) {
        key = buff.nodeData.entries[eid].key;
        rid = buff.nodeData.entries[eid].rid;
        return 0;
    } else {
        // Invalid key
        return -1;
    }
}

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node
 */
PageId BTLeafNode::getNextNodePtr()
{
    return buff.nodeData.nextNode;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
    buff.nodeData.nextNode = pid;
    return 0;
}


//------------------------------------------------------------------------------------------------------
//------------------------------------------------------------------------------------------------------

BTNonLeafNode::BTNonLeafNode() {
    // Ensuring we are in an initial valid state
    // Note that the leftmost pointer is set to
    // "invalid" and can be overwritten only by initializeRoot.
    buff.nodeData.keyCount = 0;
    buff.nodeData.pageEntries[0] = -1;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
    int status = pf.read(pid, buff.raw_buff);
    return status;
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
    int status = pf.write(pid, buff.raw_buff);
    return status;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
    return buff.nodeData.keyCount;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
    // Note that this function should insert pid to the
    // *right* of the key
{
    if(getKeyCount() >= MAX_KEYS) {
        return -1;
    } else {
        const int numKeys = getKeyCount();

        int eid;
        int status = locate(key, eid);
        if(status == 0) {
            // Locate succeeded.
            // Loop from the first non-used key to the
            // key right after 'eid'. Pull everything from
            // the left forward.
            for(int i = numKeys; i > eid; i--) {
                buff.nodeData.keyEntries[i]    = buff.nodeData.keyEntries[i-1];

                // For every key entry, we will push over the pointer
                // to its right. That means that the pointer will have
                // an index of i+1, and the pointer from which to copy
                // will have an index of i. This will work out since
                // the number of pages is one plus the number of keys
                buff.nodeData.pageEntries[i+1] = buff.nodeData.pageEntries[i];
            }
        } else {
            eid = numKeys;
        }

        // Actually place the value
        buff.nodeData.keyEntries [eid  ] = key;
        buff.nodeData.pageEntries[eid+1] = pid;

        buff.nodeData.keyCount++;

        return 0;
    }
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{
   // Just some error checking
    if(getKeyCount() != MAX_KEYS) {
        return -1;
    }

    const int half = MAX_KEYS / 2;
    midKey = buff.nodeData.keyEntries[half];
    // TODO: Worry if midKey has any meaning other than siblingKey
    // This is worrisome because it has a different name and description,
    // but the effective meaning is the same: "This key should be inserted
    // to the parent". Therefore, I am assuming it is the same as
    // siblingKey in the other insertAndSplit.


    // Before we do any work, we can update the keyCount
    buff.nodeData.keyCount = half;

    if(key >= midKey) {
        // We must insert the key into the sibling.
        // Use a loop to copy instead of memcpy, so that
        // we can save time on an insert by placing it where
        // necessary.
        bool found = false;
        int i = half, j = 0;
        for(; i < MAX_KEYS; i++, j++) {
            if(!found && buff.nodeData.keyEntries[i] >= key) {
                // We have finally found the spot we need
                // Insert the new key right here
                sibling.buff.nodeData.keyEntries [j  ] = key;
                sibling.buff.nodeData.pageEntries[j+1] = pid;

                // Increment j and let the process continue on
                j++;

                // Flag us for the future
                found = true;
            } else {
                // Just do a normal data copy
                // Note that the left-most node is left uninitialized :)
                sibling.buff.nodeData.keyEntries [j  ] = buff.nodeData.keyEntries    [i  ];
                sibling.buff.nodeData.pageEntries[j+1] = buff.nodeData.pageEntries[i+1];
            }
        }

        if(!found) {
            // We need to insert the entry at the end:
            sibling.buff.nodeData.keyEntries [j  ] = key;
            sibling.buff.nodeData.pageEntries[j+1] = pid;
        }

        // Now we must set the appropriate keyCount
        // We must include an addition of 1 for the new entry
        sibling.buff.nodeData.keyCount = MAX_KEYS - half + 1;
    } else {
        // We should not insert it in the sibling. We
        // should insert it here and do a memcpy
        memcpy(sibling.buff.nodeData.keyEntries , buff.nodeData.keyEntries+half, MAX_KEYS-half);
        memcpy(sibling.buff.nodeData.pageEntries, buff.nodeData.pageEntries+half+1, MAX_KEYS-half+1);
        sibling.buff.nodeData.keyCount = MAX_KEYS - half;

        // Now we can just call our insert routine to insert
        // the proper values. Remember that keyCount was fixed above,
        // so insert knows what to do
        int status = insert(key, pid);
        if(status != 0) return status;
    }

    return 0;
}

/*
 * !!!!!!!! ADDED !!!!!!!!!!
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (key entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the key entry number that contains a key larger than
 *                 or equal to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locate(int searchKey, int& eid)
{
    //TODO: Use binary search instead of linear
    const int numKeys = getKeyCount();
    for(int i = 0; i < numKeys; i++){
        if(buff.nodeData.keyEntries[i] >= searchKey) {
            eid = i;
            return 0;
        }
    }
    // Key with value larger than or equal to
    // searchKey was not found
    return -1;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
    int eid;
    int status = locate(searchKey, eid);
    if(status != 0) {
        // If we could not find the key, return the error.
        return status;
    }

    // Now find the appropriate pid. We will need that
    // location plus one
    pid = buff.nodeData.pageEntries[eid + 1];

    return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
    // Ensure that we truly are empty
    if(getKeyCount() != 0) {
        return -1;
    }

    buff.nodeData.pageEntries[0] = pid1;
    buff.nodeData.keyEntries [0] = key;
    buff.nodeData.pageEntries[1] = pid2;
    buff.nodeData.keyCount       = 1;

    return 0;
}
