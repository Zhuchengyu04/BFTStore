#include "Erasure.h"
#include <fstream>
#include <functional>
#include <iostream>
#include <random>

using namespace ec;
using namespace std;
using namespace rocksdb;

#define NodeAddr_Type ec::512


void Erasure::generate_ptrs(size_t data_size, uint8_t* data, erasure_bool* present, uint8_t** ptrs)
{
    size_t i;
    for (i = 0; i < ec_k + ec_m; ++i)
    {
        ptrs[i] = data + data_size * i;
    }

    for (i = 0; i < ec_k + ec_m; ++i)
    {
        present[i] = true;
    }
}

std::string Erasure::GetChunkDataKey(unsigned int coding_epoch, unsigned chunk_pos)
{
    return std::to_string(coding_epoch).append(std::to_string(chunk_pos));
}
// 
int64_t maxLen(vector<bytes> const& blocks, int64_t data_len)
{
    int64_t max = blocks[0].size();
    for (int i = 1; i < data_len; i++)
    {
        if (blocks[i].size() > max)
        {
            max = blocks[i].size();
        }
    }
    return max;
}

std::pair<byte*, int64_t> Erasure::preprocess(vector<bytes> const& blocks)
{
    // vector<vector<byte>> block_data;
    // for (int i = 0; i < ec_k; i++)  // parallelize
    // {
    //     auto blockRlp = blocks[i]->rlp();
    //     block_data.push_back(blockRlp);
    // }
    int64_t max_len = maxLen(blocks, ec_k);
    byte* blocks_rlp_data = new byte[(BLOCKS_SIZE_BYTE + max_len) * (ec_k + ec_m)];
    memset(blocks_rlp_data, 0, sizeof(byte) * (BLOCKS_SIZE_BYTE + max_len) * (ec_k + ec_m));
    for (int i = 0; i < ec_k; i++)
    {
        int64_t first_value = blocks[i].size() / (255 * 255);
        int64_t second_value = blocks[i].size() / 255 % 255;
        int64_t third_value = blocks[i].size() % 255;

        blocks_rlp_data[i * (BLOCKS_SIZE_BYTE + max_len)] = (byte)(first_value);
        blocks_rlp_data[i * (BLOCKS_SIZE_BYTE + max_len) + 1] = (byte)(second_value);
        blocks_rlp_data[i * (BLOCKS_SIZE_BYTE + max_len) + 2] = (byte)(third_value);

        for (int rlp_count = BLOCKS_SIZE_BYTE; rlp_count < (blocks[i].size() + BLOCKS_SIZE_BYTE);
             rlp_count++)
        {
            blocks_rlp_data[i * (BLOCKS_SIZE_BYTE + max_len) + rlp_count] =
                (byte)blocks[i][rlp_count - BLOCKS_SIZE_BYTE];
        }
    }
    return std::make_pair(blocks_rlp_data, BLOCKS_SIZE_BYTE + max_len);
}
static inline double GetTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1e6;
}

std::pair<byte**, int64_t> Erasure::encode(std::pair<byte*, int64_t> blocks_rlp_data)
{
    int64_t length = blocks_rlp_data.second;
    byte** ptrs = new byte*[ec_k + ec_m];
    erasure_bool* present = new erasure_bool[ec_k + ec_m];
    generate_ptrs(length, blocks_rlp_data.first, present, ptrs);

    erasure_encoder_parameters params = {ec_k + ec_m, ec_k, length};
    erasure_encoder* encoder = erasure_create_encoder(&params, ec_mode);
    erasure_encode(encoder, ptrs, ptrs + ec_k);

    erasure_destroy_encoder(encoder);


    delete present;
    return std::make_pair(ptrs, length);
}


int64_t Erasure::findSeqInSealers()
{
    for (int i = 0; i < ec_sealers.size(); i++)
    {
        if (ec_nodeid == ec_sealers[i])
        {
            return i;
        }
    }
    return -1;
}

bool Erasure::InitErasure()
{
    ec_position_in_sealers = findSeqInSealers();
    Options options;
    options.create_if_missing = true;
    Status status = DB::Open(options, ec_DBPath, &ec_db);
    assert(status.ok());
    ec_cache = std::make_shared<ec::LRUCache>(cache_capacity);
}
string DecIntToHexStr(unsigned int num)
{
    string str;
    unsigned int Temp = num / 16;
    int left = num % 16;
    if (Temp > 0)
        str += DecIntToHexStr(Temp);
    if (left < 10)
        str += (left + '0');
    else
        str += ('A' + left - 10);
    return str;
}

bool Erasure::writeDB(
    unsigned int coding_epoch, std::pair<unsigned int, byte*> chunk, int64_t chunk_size, bool flag)
{
    //写入文件格式：
    // key = coding_epoch||chunk_pos, 其中chunk_pos固定位数为节点个数的十六进制表示位数
    // value = chunk_data||digest, 其中digest固定位数为64字节
    string key = DecIntToHexStr(coding_epoch);
    string key2 = DecIntToHexStr(chunk.first);
    key.append("+").append(key2);
    string value(chunk.second, chunk.second + chunk_size);
    string digest =
        sha256(vector_ref<const byte>(
                   (const byte*)(byte*)(const_cast<char*>(value.c_str())), value.size()))
            .hex();
    
    if (flag) 
    {
        ec_cache->put(std::to_string(coding_epoch).append("+").append(to_string(chunk.first)),bytes(chunk.second,chunk.second+chunk_size));
        value.append(digest);
    }
    else
    {
        value = digest;
    }


    Status status = ec_db->Put(WriteOptions(), key, value);
    if (status.ok())
    {
        return true;
    }
    else
    {
        std::cout << "write chunk failed " << std::endl;
        return false;
    }
}

void Erasure::saveChunk(std::vector<bytes> const& blocks, int last_block_number)
{
    // Point Latest Coding Group

    unsigned int coding_epoch = (last_block_number - 1) / ec_k;
    int* chunk_set = get_distinct_chunk_set(coding_epoch);
   
    std::pair<byte**, int64_t> chunks = encode(preprocess(blocks));

    for (int i = 0; i < ec_k + ec_m; i++)
    {
        if (chunk_set[i] != ec_position_in_sealers)
        {
            bool write_res =
                writeDB(coding_epoch, std::make_pair(i, chunks.first[i]), chunks.second, false);
        }
        else
        {
            bool write_res =
                writeDB(coding_epoch, std::make_pair(i, chunks.first[i]), chunks.second, true);
        }
    }
    // parallel erasure code
    complete_coding_epoch = coding_epoch;
}

// i-th block stored by chunk_set[i]-th node
int* Erasure::get_distinct_chunk_set(unsigned int coding_epoch)
{
    std::hash<unsigned int> ec_hash;
    unsigned int seed = ec_hash(coding_epoch);
    std::mt19937 generator(seed);
    std::uniform_int_distribution<> dis(0, ec_k + ec_m - 1);
    int* chunk_set = new int[ec_k + ec_m];
    for (int j = 0; j < ec_k + ec_m; j++)
    {
        chunk_set[j] = dis(generator);
        for (int i = 0; i < j; i++)
        {
            if (chunk_set[j] == chunk_set[i])
            {
                --j;
                break;
            }
        }
    }
    return chunk_set;
}


bytes Erasure::getBlockByNumber(unsigned int block_num)
{
    unsigned int coding_epoch = (block_num - 1) / ec_k;
    unsigned int chunk_pos = (block_num - 1) % ec_k;
    bytes chunk_data =
        ec_cache->get(std::to_string(coding_epoch).append("+").append(to_string(chunk_pos)));
    if (chunk_data.size() > 0)
    {
        int64_t block_size = (unsigned int)(chunk_data[0] & 0xff) * 255 * 255 +
                             (unsigned int)(chunk_data[1] & 0xff) * 255 +
                             (unsigned int)(chunk_data[2] & 0xff);
        return bytes(chunk_data.begin() + 3, chunk_data.begin() + 3 + block_size);
    }
    else
    {
        if (computeBlocksInWhichNode(block_num) == ec_nodeid)
        {
            auto block = localReadBlock(coding_epoch, chunk_pos);
            if (block.size() == 0)
            {
                std::cout << "digest failed " << std::endl;
                return bytes();
            }

            return block;
        }
        else
        {
            return bytes();
        }
    }
}

bytes Erasure::localReadBlock(unsigned int coding_epoch, unsigned int chunk_pos)
{
    std::string get_value = readChunk(coding_epoch, chunk_pos);


    std::string chunk_data = get_value.substr(0, get_value.length() - 64);
    std::string chunk_digest = get_value.substr(get_value.length() - 64, get_value.length());
    if (sha256(vector_ref<const byte>(
                   (const byte*)(byte*)(const_cast<char*>(chunk_data.c_str())), chunk_data.size()))
            .hex() != chunk_digest)
    {
        std::cout << "localReadBlock digest failed" << std::endl;
        return bytes();
    }


    int64_t block_size = (unsigned int)(chunk_data[0] & 0xff) * 255 * 255 +
                         (unsigned int)(chunk_data[1] & 0xff) * 255 +
                         (unsigned int)(chunk_data[2] & 0xff);

    return bytes(chunk_data.begin() + 3, chunk_data.begin() + 3 + block_size);
}

NodeAddr_Type Erasure::remoteReadBlock(unsigned int block_num)
{
    unsigned int coding_epoch = (block_num - 1) / ec_k;
    unsigned int chunk_pos = (block_num - 1) % ec_k;


    int* chunk_set = get_distinct_chunk_set(coding_epoch);
    int target_idx = chunk_set[chunk_pos];
    delete chunk_set;
    NodeAddr_Type target_nodeid = ec_sealers[target_idx];
    return target_nodeid;
}

NodeAddr_Type Erasure::computeBlocksInWhichNode(int block_number)
{
    unsigned int coding_epoch = (block_number - 1) / ec_k;
    unsigned int chunk_pos = (block_number - 1) % ec_k;
    int* chunk_set = get_distinct_chunk_set(coding_epoch);
    int target_idx = chunk_set[chunk_pos];
    delete chunk_set;
    NodeAddr_Type target_nodeid = ec_sealers[target_idx];
    return target_nodeid;
}

bytes Erasure::decode(
    std::vector<bytes> const& input_data, std::vector<int> const& index, int target_block_num)
{
    unsigned int coding_epoch = (target_block_num - 1) / ec_k;
    unsigned int chunk_pos = (target_block_num - 1) % ec_k;

    // test digest
    // for (int i = 0; i < index.size(); ++i)
    // {
    //     string digest = sha256(ref(input_data[i])).hex();
    //     string db_digest = readChunk(coding_epoch, index[i]);

    //     std::cout << "db_digest = " << db_digest.substr(db_digest.length()-64) << std::endl;
    //     std::cout << "(" << coding_epoch << " " << index[i] << ");   recieve digest = " << digest
    //               << std::endl;
    // }


    erasure_bool* present = new erasure_bool[ec_k + ec_m];

    byte** ptrs = new byte*[ec_k + ec_m];

    memset(present, false, (ec_k + ec_m) * sizeof(erasure_bool));
    byte* data = new byte[(ec_k + ec_m) * input_data[0].size()];
    memset(data, 0, (ec_k + ec_m) * input_data[0].size() * sizeof(byte));
    generate_ptrs(input_data[0].size(), data, present, ptrs);

    for (int i = 0; i < index.size(); ++i)
    {
        int seq = index[i];
        present[seq] = true;
        memcpy(ptrs[seq],
            (byte*)const_cast<char*>(
                std::string(input_data[i].begin(), input_data[i].end()).c_str()),
            input_data[i].size());
        ec_cache->put(
            std::to_string(coding_epoch).append("+").append(to_string(index[i])), input_data[i]);
    }

    erasure_encoder_parameters params = {ec_k + ec_m, ec_k, input_data[0].size()};
    erasure_encoder* encoder = erasure_create_encoder(&params, ec_mode);
    erasure_recover(encoder, ptrs, present);

    erasure_destroy_encoder(encoder);
    delete present;
    std::cout << "finish decode " << std::endl;
    bytes res(ptrs[chunk_pos], ptrs[chunk_pos] + input_data[0].size());
    string digest = sha256(ref(res)).hex();
    std::cout << "decode digest = " << digest << std::endl;
    // return bytes(ptrs[chunk_pos], ptrs[chunk_pos] + input_data[0].size());
}

std::string Erasure::readChunk(unsigned int coding_epoch, unsigned int chunk_pos)
{
    std::string get_value;
    string key = DecIntToHexStr(coding_epoch);
    string key2 = DecIntToHexStr(chunk_pos);
    key.append("+").append(key2);
    Status status = ec_db->Get(ReadOptions(), key, &get_value);
    assert(status.ok());
    return get_value;
}

pair<int, int> Erasure::computeChunkPosition(int block_number)
{
    unsigned int coding_epoch = (block_number - 1) / ec_k;
    unsigned int chunk_pos = (block_number - 1) % ec_k;
    return make_pair(coding_epoch, chunk_pos);
}

vector<int> Erasure::getChunkPosByNodeIdAndEpoch(int epoch)
{
    int* chunk_set = get_distinct_chunk_set(epoch);
    vector<int> node_index_to_chunk_pos(ec_k + ec_m);
    for (int i = 0; i < ec_k + ec_m; ++i)
    {
        node_index_to_chunk_pos[chunk_set[i]] = i;
    }
    return node_index_to_chunk_pos;
}

bytes Erasure::unpackChunkToBlock(bytes const& data)
{
    int64_t block_size = (unsigned int)(data[0] & 0xff) * 255 * 255 +
                         (unsigned int)(data[1] & 0xff) * 255 + (unsigned int)(data[2] & 0xff);
    return bytes(data.begin() + 3, data.begin() + 3 + block_size);
}