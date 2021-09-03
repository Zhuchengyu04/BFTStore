#pragma once
#include "erasure-codes/liberasure.h"
#include <libdevcore/FixedHash.h>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "Common.h"
#include "BlockCache.hpp"
#include <tbb/concurrent_queue.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <set>
#include <unordered_map>


#define BLOCKS_SIZE_BYTE 3  //默认记录区块大小的字节数
#define NodeAddr_Type dev::h512

namespace ec
{
// template <class NodeAddr_Type>
class Erasure : public std::enable_shared_from_this<Erasure>
{
public:
    Erasure(int64_t _k, int64_t _m, NodeAddr_Type& _nodeid,
        std::vector<NodeAddr_Type> _sealers, std::string _path, int _cache_capacity)
      : ec_k(_k),
        ec_m(_m),
        ec_nodeid(_nodeid),
        ec_sealers(_sealers),
        ec_DBPath(_path),
        cache_capacity(_cache_capacity)
    {}
    //初始化EC模块
    bool InitErasure();

    //编解码模块
    std::pair<byte*, int64_t> preprocess(std::vector<bytes> const& blocks);
    std::pair<byte**, int64_t> encode(std::pair<byte*, int64_t> blocks_rlp_data);
    bytes decode(std::vector<bytes> const& data, std::vector<int> const& index,int target_block_num);


    bool checkChunk();
    void generate_ptrs(size_t data_size, uint8_t* data, erasure_bool* present, uint8_t** ptrs);


    //写chunk 模块
    void saveChunk(std::vector<bytes> const& blocks, int last_block_number);
    bool writeDB(unsigned int coding_epoch, std::pair<unsigned int, byte*> chunk,
        int64_t chunk_size, bool flag);
    bytes unpackChunkToBlock(bytes const& data);
    //读chunk模块
    std::string readChunk(unsigned int coding_epoch, unsigned int chunk_pos);
    //读区块模块
    bytes getBlockByNumber(unsigned int block_num);
    bytes localReadBlock(unsigned int coding_epoch, unsigned int chunk_pos);
    std::string GetChunkDataKey(unsigned int coding_epoch, unsigned chunk_pos);
    NodeAddr_Type remoteReadBlock(unsigned int block_num);

    //计算chunk_set模块
    std::pair<int*, int*> get_my_chunk_set(unsigned int coding_epoch);
    int* get_distinct_chunk_set(unsigned int coding_epoch);

    NodeAddr_Type computeBlocksInWhichNode(int block_number);
    std::pair<int,int> computeChunkPosition(int block_number);
    int64_t findSeqInSealers();
    int64_t getK() { return ec_k; }
    int64_t getM() { return ec_m; }

    unsigned int getPosition() { return ec_position_in_sealers; }
    std::vector<int> getChunkPosByNodeIdAndEpoch(int epoch);

private:
    NodeAddr_Type ec_nodeid;                                 //节点ID
    erasure_encoder_flags ec_mode = ERASURE_FORCE_ADV_IMPL;  // ec编码模式
    int64_t ec_k;                                            //数据块个数
    int64_t ec_m;                                            //校验块个数
    std::vector<NodeAddr_Type> ec_sealers;                   //所有节点地址
    unsigned int ec_position_in_sealers;  //本节点在所有节点中的相对位置
    rocksdb::DB* ec_db;                   // DB句柄
    std::string ec_DBPath;                // DB路径
    std::shared_ptr<ec::LRUCache> ec_cache;
    int cache_capacity = 10;
    int64_t complete_coding_epoch = -1;  //已完成ECepoch
};
}  // namespace ec
