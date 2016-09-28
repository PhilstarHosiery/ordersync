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
 * File:   dbf.h
 * Author: Hyun Suk Noh <hsnoh@philstar.biz>
 *
 * Created on July 26, 2016, 4:27 PM
 */

#ifndef DBF_H
#define DBF_H

#include <iostream>
#include <cstdio>

#include <stdlib.h>

using namespace std;


/* This should be big enough to hold most of the varchars and memo fields
 * that you'll be processing.  If a given piece of data won't fit in a
 * buffer of this size, then a temporary buffer will be allocated for it. */
#define STATICBUFFERSIZE 1024 * 1024 * 4

/* Attempt to read approximately this many bytes from the .dbf file at once.
 * The actual number may be adjusted up or down as appropriate. */
#define DBFBATCHTARGET 1024 * 1024 * 16

/* Old versions of FoxPro (and probably other programs) store the memo file
 * record number in human-readable ASCII. Newer versions of FoxPro store it
 * as a 32-bit packed int. */
#define NUMERICMEMOSTYLE 0
#define PACKEDMEMOSTYLE 1

/* Don't edit this! It's defined in the XBase specification. */
#define XBASEFIELDNAMESIZE 11

/* This is the maximum size a generated PostgreSQL column size can possibly
 * be. It's used when making unique versions of duplicated field names.
 *
 *    11 bytes for the maximum XBase field name length
 *    1 byte for a "_" separator
 *    5 bytes for the numeric "serial number" portion
 *    1 byte for the trailing \0
 */
#define MAXCOLUMNNAMESIZE (XBASEFIELDNAMESIZE + 7)

// static char staticbuf[STATICBUFFERSIZE + 1];

typedef struct {
    int8_t signature;
    int8_t year;
    int8_t month;
    int8_t day;
    uint32_t recordcount;
    uint16_t headerlength;
    uint16_t recordlength;
    int8_t reserved1[2];
    int8_t incomplete;
    int8_t encrypted;
    int8_t reserved2[4]; /* Free record thread */
    int8_t reserved3[8]; /* Reserved for multi-user dBASE */
    int8_t mdx;
    int8_t language;
    int8_t reserved4[2];
} DBFHEADER;

typedef struct {
    char name[XBASEFIELDNAMESIZE];
    char type;
    int32_t memaddress;
    uint8_t length;
    uint8_t decimals;
    int16_t flags; /* Reserved for multi-user dBase */
    char workareaid;
    char reserved1[2]; /* Reserved for multi-user dBase */
    char setfields;
    char reserved2[7];
    char indexfield;
} DBFFIELD;

typedef struct {
    char nextblock[4];
    char reserved1[2];
    char blocksize[2];
    char reserved2[504];
} MEMOHEADER;

typedef struct {
    char *formatstring;
    int memonumbering;
} PGFIELD;

static void exitwitherror(const std::string message, const int systemerror) {
    /* Print the given error message to stderr, then exit.  If systemerror
     * is true, then use perror to explain the value in errno. */
    if (systemerror) {
        perror(message.c_str());
    } else {
        // fprintf(stderr, "%s\n", message);
        std::cerr << message << endl;
    }
    exit(EXIT_FAILURE);
}




/* Endian-specific code.  Define functions to convert input data to the
 * required form depending on the endianness of the host architecture. */

#define SWAP8BYTES(rightend, wrongendcharptr)   \
    const char *src = wrongendcharptr + 7;      \
    memcpy((char *) &rightend    , src--, 1);   \
    memcpy((char *) &rightend + 1, src--, 1);   \
    memcpy((char *) &rightend + 2, src--, 1);   \
    memcpy((char *) &rightend + 3, src--, 1);   \
    memcpy((char *) &rightend + 4, src--, 1);   \
    memcpy((char *) &rightend + 5, src--, 1);   \
    memcpy((char *) &rightend + 6, src--, 1);   \
    memcpy((char *) &rightend + 7, src  , 1);

#define SWAPANDRETURN8BYTES(wrongendcharptr)   \
    int64_t rightend;                          \
    SWAP8BYTES(rightend, wrongendcharptr)      \
    return rightend;

#define SWAPANDRETURN4BYTES(wrongendcharptr)   \
    const char *src = wrongendcharptr + 3;     \
    int32_t rightend;                          \
    memcpy((char*) &rightend    , src--, 1);   \
    memcpy((char*) &rightend + 1, src--, 1);   \
    memcpy((char*) &rightend + 2, src--, 1);   \
    memcpy((char*) &rightend + 3, src  , 1);   \
    return rightend;

#define SWAPANDRETURN2BYTES(wrongendcharptr)   \
    const char *src = wrongendcharptr + 1;     \
    int16_t rightend;                          \
    memcpy((char*) &rightend    , src--, 1);   \
    memcpy((char*) &rightend + 1, src  , 1);   \
    return rightend;

/* Integer-to-integer */

static int64_t nativeint64_t(const int64_t rightend) {
    /* Leave a 64-bit integer alone */
    return rightend;
}

static int64_t swappedint64_t(const int64_t wrongend) {
    /* Change the endianness of a 64-bit integer */
    SWAPANDRETURN8BYTES(((char *) &wrongend))
}

static uint32_t nativeint32_t(const uint32_t rightend) {
    /* Leave a 32-bit integer alone */
    return rightend;
}

static int32_t swappedint32_t(const int32_t wrongend) {
    /* Change the endianness of a 32-bit integer */
    SWAPANDRETURN4BYTES(((char*) &wrongend))
}

static int16_t nativeint16_t(const int16_t rightend) {
    /* Leave a 16-bit integer alone */
    return rightend;
}

static int16_t swappedint16_t(const int16_t wrongend) {
    /* Change the endianness of a 16-bit integer */
    SWAPANDRETURN2BYTES(((char*) &wrongend))
}

/* String-to-integer */

static int64_t snativeint64_t(const char *buf) {
    /* Interpret the first 8 bytes of buf as a 64-bit int */
    int64_t output;
    memcpy(&output, buf, 8);
    return output;
}

static int64_t sswappedint64_t(const char *buf) {
    /* The byte-swapped version of snativeint64_t */
    SWAPANDRETURN8BYTES(buf)
}

static int32_t snativeint32_t(const char *buf) {
    /* Interpret the first 4 bytes of buf as a 32-bit int */
    int32_t output;
    memcpy(&output, buf, 4);
    return output;
}

static int32_t sswappedint32_t(const char *buf) {
    /* The byte-swapped version of snativeint32_t */
    SWAPANDRETURN4BYTES(buf)
}

static int16_t snativeint16_t(const char *buf) {
    /* Interpret the first 2 bytes of buf as a 16-bit int */
    int16_t output;
    memcpy(&output, buf, 2);
    return output;
}

static int16_t sswappedint16_t(const char *buf) {
    /* The byte-swapped version of snativeint16_t */
    SWAPANDRETURN2BYTES(buf)
}

#ifdef WORDS_BIGENDIAN
#define bigint64_t     nativeint64_t
#define littleint64_t  swappedint64_t

#define bigint32_t     nativeint32_t
#define littleint32_t  swappedint32_t

#define bigint16_t     nativeint16_t
#define littleint16_t  swappedint16_t

#define sbigint64_t    snativeint64_t
#define slittleint64_t sswappedint64_t

#define sbigint32_t    snativeint32_t
#define slittleint32_t sswappedint32_t

#define sbigint16_t    snativeint16_t
#define slittleint16_t sswappedint16_t

static double sdouble(const char *buf) {

    /* Doubles are stored as 64-bit little-endian, so swap ends */
    union {
        int64_t asint64;
        double asdouble;
    } inttodouble;

    SWAP8BYTES(inttodouble.asint64, buf)
    return inttodouble.asdouble;
}
#else
#define bigint64_t     swappedint64_t
#define littleint64_t  nativeint64_t

#define bigint32_t     swappedint32_t
#define littleint32_t  nativeint32_t

#define bigint16_t     swappedint16_t
#define littleint16_t  nativeint16_t

#define sbigint64_t    sswappedint64_t
#define slittleint64_t snativeint64_t

#define sbigint32_t    sswappedint32_t
#define slittleint32_t snativeint32_t

#define sbigint16_t    sswappedint16_t
#define slittleint16_t snativeint16_t

static double sdouble(const char *buf) {
    /* Interpret the first 8 bytes of buf as a double */
    double output;
    memcpy(&output, buf, 8);
    return output;
}

#endif


#endif /* DBF_H */

