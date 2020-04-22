#include "heap_storage.h"
#include <cstring>
#include <exception>
#include <map>
#include <utility>
#include <vector>
using namespace std;


typedef u_int16_t u16;
typedef u_int32_t u32;


bool test_heap_storage() {
	cout << "hello test" << endl;
	return true;
}

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id){
    u16 loc, size;
    get_header(size, loc, record_id);
    if(loc == 0){
        // tombstone
        return NULL;
    }
    char *right_size_bytes = new char[size];
    memcpy(right_size_bytes, this->address(loc), size);
    Dbt* r = new Dbt(right_size_bytes, size);
    return r;
}

void SlottedPage::put(RecordID record_id, const Dbt &data){
	u16 loc, size;
    get_header(size, loc, record_id);
    u16 new_size = data.get_size();
    if(new_size > size){
        u16 extra = new_size - size;
        if(!has_room(extra)){
            throw DbBlockNoRoomError("not enough room for new record");
        }
        slide(loc + new_size, loc + size);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id){
	u16 size, loc;
	get_header(size, loc, record_id);
	put_header(record_id, 0, 0);
	slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void){
    RecordIDs *allIDs ;
    for(int i = 1; i < this->num_records + 1; i ++){
        if(get(i) != NULL){
            allIDs->push_back(i);
        }
    }
    return allIDs;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

void SlottedPage::slide(u16 start, u16 end){
	u16 shift = end - start;
    if(shift == 0){
        return ;
    }

    // slide data
    memcpy(this->address(this->end_free + 1), this->address(this->end_free + 1 + shift), end - this->end_free - shift);

    RecordIDs *allIDs = ids();
    auto it = allIDs->begin();
    while(it != allIDs->end()){
        u16 id = *it;
        u16 size, loc;
        get_header(size, loc, id);
        if(loc <= start){
            loc += shift;
            put_header(id, size, loc);
        }
        it ++;
    }
    this->end_free += shift;
    put_header();
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id){
	size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}


bool SlottedPage::has_room(u16 size){
	u16 available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}


/*****************************************Heap File***************************************************************/

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[SlottedPage::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}





/*****************************************Heap Table***************************************************************/
//todo: HEAPTABLE METHODS
/*HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes){
		
}

void HeapTable::create(){
	
}

void HeapTable::create_if_not_exists(){
	
}

void HeapTable::drop(){
	
}

void HeapTable::open(){
	
}

void HeapTable::close(){
	
}

Handle HeapTable::insert(const ValueDict *row){
	Handles* handles = new Handles();
	//todo: everything else
	return handles;
}

Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}*/











