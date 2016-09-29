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
 * File:   dbfReader.h
 * Author: Hyun Suk Noh <hsnoh@philstar.biz>
 *
 * Created on July 26, 2016, 4:12 PM
 */

#ifndef DBFREADER_H
#define DBFREADER_H

#include <cstdlib>
#include <string>

#include "dbf.h"

using namespace std;

class dbfReader {
private:
    FILE *dbffile;
    DBFHEADER dbfheader;
    DBFFIELD *fields;

    // unsigned int index;
    size_t fieldcount; /* Number of fields for this DBF file */
    unsigned int recordbase; /* The first record in a batch of records */
    unsigned int dbfbatchsize; /* How many DBF records to read at once */
    unsigned int batchindex; /* The offset inside the current batch of DBF records */

    int *fieldpos; /* Field starting positions in a record */

    char *inputbuffer;
    char *bufoffset;
    size_t blocksread;

    bool is_open;

public:
    dbfReader();
    dbfReader(string filename);
    dbfReader(const dbfReader& orig);
    virtual ~dbfReader();

    void open(string filename);
    void close();

    void reset();
    bool next();

    // string getString(string field);
    string getString(unsigned int fieldnum);
    // int getInt(string field);
    bool isClosedRow();
    
    int getFieldIndex(string fieldname);

private:
    bool strequali(string str1, string str2);
    string trimGet(char* src, int len);
};

#endif /* DBFREADER_H */

