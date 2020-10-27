// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"
#include <iostream>

namespace leveldb {

    struct TableAndFile {
        RandomAccessFile* file;
        Table* table;
    };

    struct TableAndHandle {
        Cache* file_cache;
        Cache::Handle* filehandle;
        Table* table;
    };
    static void DeleteFile(const Slice& key, void* f) {
        RandomAccessFile *file = reinterpret_cast<RandomAccessFile*>(f);
        delete file;
    }
    static void DeleteTableReleaseHandle(const Slice& key, void* value){
        TableAndHandle* th = reinterpret_cast<TableAndHandle*>(value);
        th->file_cache->Release(th->filehandle);
        delete th->table;
        delete th;
    }

    static void DeleteEntry(const Slice& key, void* value) {
        TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
        delete tf->table;
        delete tf->file;
        delete tf;
    }

    static void UnrefEntry(void* arg1, void* arg2) {
        Cache* cache = reinterpret_cast<Cache*>(arg1);
        Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
        cache->Release(h);
    }

    TableCache::TableCache(const std::string& dbname,
            const Options& options,
            int entries)
        : env_(options.env),
        dbname_(dbname),
        options_(options),
        cache_(NewLRUCache(64*entries))
        ,file_cache_(NewLRUCache(entries))

        {
        }

    TableCache::~TableCache() {
        delete cache_;
        delete file_cache_;

    }

    Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, uint64_t endOffset, Cache::Handle** handle
            ) {

        Status s;
        char filebuf[sizeof(file_number)];
        EncodeFixed64(filebuf, file_number);
        Slice fkey(filebuf, sizeof(file_number));

        Cache::Handle* handle2;
        handle2= file_cache_->Lookup(fkey);
        RandomAccessFile* file = nullptr;
        Table* table = nullptr;

        if(handle2 == nullptr)
        {
            std::string fname = TableFileName(dbname_, file_number);

            s = env_->NewRandomAccessFile(fname, &file);
            if (!s.ok()) {
                std::string old_fname = SSTTableFileName(dbname_, file_number);
                if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
                    s = Status::OK();
                }
            }
            handle2 = file_cache_->Insert(fkey, file, 1, &DeleteFile);
        }
        else{
            file = reinterpret_cast<RandomAccessFile*>(file_cache_->Value(handle2));
        }

        if(!s.ok()){
            // file open error
            return s;
        }

        char buf[sizeof(file_number)+sizeof(endOffset)];
        EncodeFixed64(buf, endOffset);
        EncodeFixed64(buf+sizeof(endOffset), file_number);

        Slice key(buf, sizeof(file_number)+sizeof(endOffset));
        
        // find table
        *handle = cache_->Lookup(key);

        if (*handle == nullptr) {
            if (s.ok()) {
                if(endOffset==-1)
                    s = Table::Open(options_, file, file_size,file_size,&table);
                else
                    s = Table::Open(options_, file, file_size,endOffset,&table);
            }

            if (!s.ok()) {
                assert(table == nullptr);
            } else {
                TableAndHandle* th = new TableAndHandle;
                th->file_cache = file_cache_;
                th->filehandle = handle2;
                th->table = table;
                *handle = cache_->Insert(key, th, 1, &DeleteTableReleaseHandle);
            }
        }
        else{
            file_cache_->Release(handle2);
        }

        return s;
    }

    Iterator* TableCache::NewIterator(const ReadOptions& options,
            uint64_t file_number,
            uint64_t file_size,
            uint64_t endOffset,
            Table** tableptr) {

        if (tableptr != nullptr) {
            *tableptr = nullptr;
        }

        Cache::Handle* handle = nullptr;
        Status s = FindTable(file_number, file_size, endOffset, &handle);
        if (!s.ok()) {
            return NewErrorIterator(s);
        }

        Table* table = reinterpret_cast<TableAndHandle*>(cache_->Value(handle))->table;
        Iterator* result = table->NewIterator(options);
        result->RegisterCleanup(&UnrefEntry, cache_, handle);

        if (tableptr != nullptr) {
            *tableptr = table;
        }

        return result;
    }

    Status TableCache::Get(const ReadOptions& options,
            uint64_t file_number,
            uint64_t file_size,
            uint64_t endOffset,
            const Slice& k,
            void* arg,
            void (*saver)(void*, const Slice&, const Slice&)) {
        Cache::Handle* handle = nullptr;
        Status s = FindTable(file_number, file_size, endOffset,&handle);
        if (s.ok()) {
            Table* t = reinterpret_cast<TableAndHandle*>(cache_->Value(handle))->table;
            s = t->InternalGet(options, k,
                    endOffset==-1 ? 0 : endOffset - file_size,
                    arg, saver);
            cache_->Release(handle);
        }
        return s;
    }

    void TableCache::FileEvict(uint64_t file_number){
        char fbuf[sizeof(file_number)];
        EncodeFixed64(fbuf,file_number);
        file_cache_->Erase(Slice(fbuf,sizeof(fbuf)));
    }

    void TableCache::Evict(uint64_t file_number, uint64_t endOffset) {
        char buf[sizeof(file_number)+sizeof(endOffset)];
        EncodeFixed64(buf, endOffset);
        EncodeFixed64(buf+sizeof(endOffset), file_number);
        cache_->Erase(Slice(buf, sizeof(buf)));
    }

}  // namespace leveldb
