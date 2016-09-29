/* DBF Library - Library to read DBF files                               */
/* Copyright (C) 2016  Hyun Suk Noh <hsnoh@philstar.biz>                 */
/* Mostly taken and modified from PgDBF by Kirk Strauser                 */
/* <kirk@strauser.com>.                                                  */
/*                                                                       */
/* This program is free software: you can redistribute it and/or modify  */
/* it under the terms of the GNU General Public License as published by  */
/* the Free Software Foundation, either version 3 of the License, or     */
/* (at your option) any later version.                                   */
/*                                                                       */
/* This program is distributed in the hope that it will be useful,       */
/* but WITHOUT ANY WARRANTY; without even the implied warranty of        */
/* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         */
/* GNU General Public License for more details.                          */
/*                                                                       */
/* You should have received a copy of the GNU General Public License     */
/* along with this program.  If not, see <http://www.gnu.org/licenses/>. */

/* 
 * File:   dbfReader.cpp
 * Author: Hyun Suk Noh <hsnoh@philstar.biz>
 * 
 * Created on July 26, 2016, 4:12 PM
 */
#include <cstdio>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "dbf.h"
#include "dbfReader.h"

using namespace std;

dbfReader::dbfReader() {
    is_open = false;
}

dbfReader::dbfReader(string filename) {
    open(filename);
}

dbfReader::dbfReader(const dbfReader& orig) {

}

dbfReader::~dbfReader() {

}

void dbfReader::open(string filename) {
    size_t dbffieldsize;

    int skipbytes; /* The length of the Visual FoxPro DBC in this file (if there is one) */
    int fieldarraysize; /* The length of the field descriptor array */
    size_t fieldnum; /* The current field being processed */
    uint8_t terminator; /* Testing for terminator bytes */

    /* Get the DBF header */
    dbffile = fopen(filename.c_str(), "rb");
    if (dbffile == NULL) {
        exitwitherror("Unable to open the DBF file", 1);
    }
    if (setvbuf(dbffile, NULL, _IOFBF, DBFBATCHTARGET)) {
        exitwitherror("Unable to set the buffer for the dbf file", 1);
    }
    if (fread(&dbfheader, sizeof (dbfheader), 1, dbffile) != 1) {
        exitwitherror("Unable to read the entire DBF header", 1);
    }

    if (dbfheader.signature == 0x30) {
        /* Certain DBF files have an (empty?) 263-byte buffer after the header
         * information.  Take that into account when calculating field counts
         * and possibly seeking over it later. */
        skipbytes = 263;
    } else {
        skipbytes = 0;
    }

    /* Calculate the number of fields in this file */
    dbffieldsize = sizeof (DBFFIELD);
    fieldarraysize = littleint16_t(dbfheader.headerlength) - sizeof (dbfheader) - skipbytes - 1;
    if (fieldarraysize % dbffieldsize == 1) {
        /* Some dBASE III files include an extra terminator byte after the
         * field descriptor array.  If our calculations are one byte off,
         * that's the cause and we have to skip the extra byte when seeking
         * to the start of the records. */
        skipbytes += 1;
        fieldarraysize -= 1;
    } else if (fieldarraysize % dbffieldsize) {
        exitwitherror("The field array size is not an even multiple of the database field size", 0);
    }
    fieldcount = fieldarraysize / dbffieldsize;

    
    try {
        /* Fetch the description of each field */
        fields = new DBFFIELD [fieldcount];
        
        if (fread(fields, dbffieldsize, fieldcount, dbffile) != fieldcount) {
            exitwitherror("Unable to read all of the field descriptions", 1);
        }

        // Compute field starting positions
        fieldpos = new int [fieldcount];
        int tmp_pos = 1;
        for (fieldnum = 0; fieldnum < fieldcount; fieldnum++) {
            fieldpos[fieldnum] = tmp_pos;
            tmp_pos += fields[fieldnum].length;
        }

        /* Check for the terminator character */
        if (fread(&terminator, 1, 1, dbffile) != 1) {
            exitwitherror("Unable to read the terminator byte", 1);
        }
        if (terminator != 13) {
            exitwitherror("Invalid terminator byte", 0);
        }

        /* Skip the database container if necessary */
        if (fseek(dbffile, skipbytes, SEEK_CUR)) {
            exitwitherror("Unable to seek in the DBF file", 1);
        }

        /* Make sure we're at the right spot before continuing */
        if (ftell(dbffile) != littleint16_t(dbfheader.headerlength)) {
            exitwitherror("At an unexpected offset in the DBF file", 0);
        }

        dbfbatchsize = DBFBATCHTARGET / littleint16_t(dbfheader.recordlength);
        if (!dbfbatchsize) {
            dbfbatchsize = 1;
        }
        
        inputbuffer = new char [littleint16_t(dbfheader.recordlength) * dbfbatchsize];
    } catch(std::bad_alloc& ba) {
        exitwitherror(string("Unable to allocate memory from heap: ") + ba.what(), 1);
    }
    
    
    is_open = true;

    reset();
}

void dbfReader::close() {
    delete[] inputbuffer;
    delete[] fieldpos;
    delete[] fields;
    
    fclose(dbffile);
    
    is_open = false;
}

void dbfReader::reset() {
    if (!is_open) {
        exitwitherror("DBF file is not loaded", 1);
    }

    // First batch loading
    blocksread = fread(inputbuffer, littleint16_t(dbfheader.recordlength), dbfbatchsize, dbffile);
    if (blocksread != dbfbatchsize &&
            recordbase + blocksread < littleint32_t(dbfheader.recordcount)) {
        exitwitherror("Unable to read an entire record", 1);
    }

    recordbase = 0;
    batchindex = -1;
}

bool dbfReader::next() {
    if (!is_open) {
        exitwitherror("DBF file is not loaded", 1);
    }

    batchindex++;
    // if already past last record, return false
    if (recordbase + batchindex >= littleint32_t(dbfheader.recordcount)) {
        return false;
    }

    // if batchindex already past blocksread, load next
    if (batchindex >= blocksread) {
        blocksread = fread(inputbuffer, littleint16_t(dbfheader.recordlength), dbfbatchsize, dbffile);

        batchindex = 0;
        recordbase += dbfbatchsize;

        if (blocksread != dbfbatchsize && recordbase + blocksread < littleint32_t(dbfheader.recordcount)) {
            exitwitherror("Unable to read an entire record", 1);
        }
    }

    bufoffset = inputbuffer + littleint16_t(dbfheader.recordlength) * batchindex;
    return true;
}

string dbfReader::getString(unsigned int fieldnum) {
    if (!is_open) {
        exitwitherror("DBF file is not loaded", 1);
    }

    if (/*fieldnum < 0 || */ fieldnum >= fieldcount) { // fieldnum >= 0 implicit for unsigned int
        exitwitherror("Field number out of bound", 1);
    }

    int len = fields[fieldnum].length;

    return trimGet(bufoffset + fieldpos[fieldnum], len);
}

bool dbfReader::isClosedRow() {
    return bufoffset[0] == '*';
}

int dbfReader::getFieldIndex(string fieldname) {
    int index = -1;
    string tmp;
    
    for (int i=0 ; i<fieldcount ; i++) {
        tmp = trimGet(fields[i].name, strlen(fields[i].name));

        if (strequali(tmp, fieldname)) {
            index = i;
            break;
        }
    }
    
    return index;
}

bool dbfReader::strequali(string str1, string str2) {
    if(str1.length() != str2.length()) {
        return false;
    }
    
    for(int i=0 ; i < str1.length() ; i++) {
        if (tolower(str1[i]) != tolower(str2[i])) {
            return false;
        }
    }
    
    return true;
}

string dbfReader::trimGet(char* src, int len) {
    char temp[256];

    // Visual FoxPro non-memo field limit is 254 chars
    if (len > 254) {
        len = 254;
    }

    int i = 0;
    while (i < len && isspace(src[i])) {
        i++;
    }

    int j = len - 1;
    while (j > i && isspace(src[j])) {
        j--;
    }

    strncpy(temp, src + i, j - i + 1);
    temp[j -i + 1] = 0;

    if(j - i + 1 == 0)
        return "";
    else
        return string(temp);
}
