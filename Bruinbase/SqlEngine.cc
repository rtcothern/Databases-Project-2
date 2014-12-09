/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
#include <limits> // for std::numeric_limits
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include <sys/stat.h>

using namespace std;

// external functions and variables for load file and sql command parsing
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

inline bool fileExists(const std::string& name) {
  struct stat buffer;
  return (stat (name.c_str(), &buffer) == 0);
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;
  string value;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  // Check if index exists
  // Open index if so
  // Set validIndex if it worked
  BTreeIndex index;
  bool validIndex;
  if (fileExists(table+".idx")){
    RC result = index.open(table+".idx", 'r');
    if(result != 0) return result;
    validIndex = true;
  } else {
    validIndex = false;
  }

  bool useIndex     = false;

  bool noResults    = false;

  bool equalsExists = false;
  int  equalsKey    = -1;
  bool gtExists     = false;
  bool gtNotGte     = true; // Greater-than, not Greater-than-or-equal
  int  gtKey        = std::numeric_limits<int>::min();
  bool ltExists     = false;
  bool ltNotLte     = true; // Less-than, not Less-than-or-equal
  int  ltKey        = std::numeric_limits<int>::max();

  bool needValue    = attr == 2 || attr == 3;

  if (cond.size() == 0 && attr == 4){
    // IndexCursor cursor;
    // index.locate(std::numeric_limits<int>::max(),cursor);
    useIndex = true;
  }

  for (unsigned i = 0; i < cond.size(); i++) {
    if(cond[i].attr == 1) {
      int currentKey = atoi(cond[i].value);
      if(cond[i].comp == SelCond::NE) {
        if(equalsExists && equalsKey == currentKey) {
            noResults = true;
        }
      } else {
        useIndex = true;
        switch(cond[i].comp) {
          case SelCond::EQ:
            if(equalsExists) {
              if(equalsKey != currentKey) {
                noResults = true;
              }
            } else {
              equalsExists = true;
              equalsKey = currentKey;
            }
            break;
          case SelCond::GT:
            if(!gtExists || currentKey >= gtKey) {
              // If the original was a GT, then we can set it because we will
              // either smallen the range or keep it the same. If the original
              // was a GE, then we will only smallen the range (and will leave
              // it alone if it is the same.)
              gtExists = true;
              gtKey = currentKey;
              gtNotGte = true;
            }
            break;
          case SelCond::LT:
            if(!ltExists || currentKey <= ltKey) {
              // If the original was a LT, then we can set it because we will
              // either smallen the range or keep it the same. If the original
              // was a LE, then we will only smallen the range (and will leave
              // it alone if it is the same.)
              ltExists = true;
              ltKey = currentKey;
              ltNotLte = true;
            }
            break;
          case SelCond::GE:
            if(!gtExists || currentKey > gtKey) {
              // Here, we check if currentKey > gtKey. If it was a GT,
              // then we should change it only if it was greater than the
              // original limit. In this case, it may or may not smallen
              // the range, but it will surely not increase it. If it was
              // originally a GE, then again we need to change it only when
              // the value will be different.
              gtExists = true;
              gtKey = currentKey;
              gtNotGte = false;
            }
            break;
          case SelCond::LE:
            if(!ltExists || currentKey < ltKey) {
              ltExists = true;
              ltKey = currentKey;
              ltNotLte = true;
            }
            break;
        }
      }
    } else if(cond[i].attr == 2) {
      // There is a condition on the value, which means we will
      // need to read the value for every iteration in the index
      needValue = true;
    }

    if(noResults) break;
  }

  // Now we have gotten our expected ranges. We can rule out some work
  // if we compare the ends of the ranges.
  if(!noResults) {
    // Only do this if we have some results available
    if(gtExists) {
      if(    gtKey > ltKey // Such as x >= 5, x <= 4
         || ((gtNotGte || ltNotLte) && gtKey == ltKey) // Such as x >= 5, x < 5
         || ((gtNotGte && ltNotLte) && ltKey-gtKey == 1) // Such as x > 4, x < 5
         || (gtNotGte && gtKey == std::numeric_limits<int>::max())) { // x > 2147483647
        noResults = true;
      }
    }

    if(ltExists) {
      if(ltNotLte && ltKey == std::numeric_limits<int>::min()) { // x < -2147483648
        noResults = true;
      }
    }
  }

  int count = 0;

  if(!noResults) {
    if(validIndex && useIndex) {
      int minKey;
      int maxKey;
      if(equalsExists) {
        minKey = equalsKey;
        maxKey = equalsKey;
      } else {
        if(gtExists) {
          if(gtNotGte) {
            // We should convert the GT into a GTE by adding one.
            // We have already confirmed that the gtKey is not int_max
            minKey = gtKey + 1;
          } else {
            minKey = gtKey;
          }
        } else {
          // We have a range that doesn't begin,
          // so we will use the minimum int
          minKey = std::numeric_limits<int>::min();
        }
        if(ltExists) {
          if(ltNotLte) {
            maxKey = ltKey - 1;
          } else {
            maxKey = ltKey;
          }
        } else {
          maxKey = std::numeric_limits<int>::max();
        }
      }

      IndexCursor cursor;
      rc = index.locate(minKey, cursor);
      if(rc != 0) {
        // TODO: in case nothing found, don't die :)
        fprintf(stderr, "Error code %d after locate attempt for %d.\n", rc, minKey);
        goto exit_select;
      }
      for(;;) {
        if(cursor.pid == -1) {
          // No more pids
          break;
        }

        rc = index.readForward(cursor, key, rid);
        if(rc != 0) {
          fprintf(stderr, "Error code %d after readForward attempt with key %d.\n", rc, key);
          goto exit_select;
        }
        if(key > maxKey) {
          // Past the max range
          break;
        }

        if(needValue) {
          rc = rf.read(rid, key, value);
          if(rc != 0) {
            fprintf(stderr, "Error code %d after read attempt with key %d.\n", rc, key);
            goto exit_select;
          }
        }

        // check the value/not-equal conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
          if(cond[i].attr == 1 && cond[i].comp == SelCond::NE) {
            // We only need to check the not-equals for the key conditions
            if(atoi(cond[i].value) == key) {
              // The current key is the not-equal key
              goto next_tup2;
            }
          } else if(cond[i].attr == 2) {
            // We know that 'value' has been set, since we previously
            // checked for any conditions with attribute 2
            diff = strcmp(value.c_str(), cond[i].value);
            // skip the tuple if any condition is not met
            switch (cond[i].comp) {
              case SelCond::EQ:
                if (diff != 0) goto next_tup2;
                break;
              case SelCond::NE:
                if (diff == 0) goto next_tup2;
                break;
              case SelCond::GT:
                if (diff <= 0) goto next_tup2;
                break;
              case SelCond::LT:
                if (diff >= 0) goto next_tup2;
                break;
              case SelCond::GE:
                if (diff < 0) goto next_tup2;
                break;
              case SelCond::LE:
                if (diff > 0) goto next_tup2;
                break;
            }
          }
        }

        // the condition is met for the tuple.
        // increase matching tuple counter
        count++;

        // print the tuple
        if(attr == 1) {
          fprintf(stdout, "%d\n", key);
        } else if(attr == 2) {
           fprintf(stdout, "%s\n", value.c_str());
        } else if(attr == 3) {
           fprintf(stdout, "%d '%s'\n", key, value.c_str());
        }

        next_tup2:
        if(key == maxKey) {
          // We already made sure key was not greater than maxKey
          // But if it is equal then we want to finish now since
          // there are no duplicates
          break;
        }
      }
    } else {
      // scan the table file from the beginning
      rid.pid = rid.sid = 0;
      while (rid < rf.endRid()) {
        // read the tuple
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        }

        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
          // compute the difference between the tuple value and the condition value
          switch (cond[i].attr) {
            case 1:
              diff = key - atoi(cond[i].value);
              break;
            case 2:
              diff = strcmp(value.c_str(), cond[i].value);
              break;
          }

          // skip the tuple if any condition is not met
          switch (cond[i].comp) {
            case SelCond::EQ:
              if (diff != 0) goto next_tuple;
              break;
            case SelCond::NE:
              if (diff == 0) goto next_tuple;
              break;
            case SelCond::GT:
              if (diff <= 0) goto next_tuple;
              break;
            case SelCond::LT:
              if (diff >= 0) goto next_tuple;
              break;
            case SelCond::GE:
              if (diff < 0) goto next_tuple;
              break;
            case SelCond::LE:
              if (diff > 0) goto next_tuple;
              break;
          }
        }

        // the condition is met for the tuple.
        // increase matching tuple counter
        count++;

        // print the tuple
        switch (attr) {
          case 1:  // SELECT key
            fprintf(stdout, "%d\n", key);
            break;
          case 2:  // SELECT value
            fprintf(stdout, "%s\n", value.c_str());
            break;
          case 3:  // SELECT *
            fprintf(stdout, "%d '%s'\n", key, value.c_str());
            break;
        }

        // move to the next tuple
        next_tuple:
        ++rid;
      }
    }
  }

  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  index.close();
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  fprintf(stderr, "Load called!\n");
  RecordFile rf;
  RecordId rid;
  int result;

  BTreeIndex possibleIndex;
  if(index) {
    result = possibleIndex.open(table + ".idx", 'w');
    if(result != 0) return result;
  }

  result = rf.open(table + ".tbl", 'w');
  if(result != 0) return result;

  // TODO: Care about file not existing/filesystem errors
  ifstream input(loadfile.c_str());
  if(!input) {
    // Error
    return -5;
  }
  string line;
  int key;
  string value;
  while(getline(input, line)) {
    result = parseLoadLine(line, key, value);
    if(result != 0) return result;

    result = rf.append(key, value, rid);
    if(result != 0) {
      fprintf(stderr, "Append of key %d failed in load\n", key);
      return result;
    }

    if(index) {
      result = possibleIndex.insert(key, rid);
      if(result != 0) {
        fprintf(stderr, "Insert of key %d, pid %d failed in load\n", key, rid.pid);
        return result;
      }
    }
  }
  if(index) {
    result = possibleIndex.close();
    if(result != 0) return result;
  }
  return rf.close();
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;

    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');

    // if there is nothing left, set the value to empty string
    if (c == 0) {
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
