/**
 * @file   heap_storage.cpp
 * @brief  the implementation file for HeapTable, HeapFile and SlottedPage classes
 * @authors Ethan Guttman, XingZheng
 */
#include "heap_storage.h"
#include <cstring>
#include <exception>
#include <map>
#include <utility>
#include <vector>
using namespace std;


typedef u_int16_t u16;
typedef u_int32_t u32;

/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 */
bool assertion_failure(string message) {
    cout << "FAILED TEST: " << message << endl;
    return false;
}

/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise 
 */
bool test_slotted_page() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);
    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assertion_failure("add id 1");
    // get it back
    Dbt *get_dbt = slot.get(id);
    string expected(rec1, sizeof(rec1));
    string actual((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back " + actual);
    delete get_dbt;
    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assertion_failure("add id 2");
    // get it back
    get_dbt = slot.get(id);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back " + actual);
    delete get_dbt;
    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back after expanding put of 1 " + actual);
    delete get_dbt;
    get_dbt = slot.get(1);
    expected = string(rec1_rev, sizeof(rec1_rev));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back after expanding put of 1 " + actual);
    delete get_dbt;
    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back after contracting put of 1 " + actual);
    delete get_dbt;
    get_dbt = slot.get(1);
    expected = string(rec1, sizeof(rec1));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back after contracting put of 1 " + actual);
    delete get_dbt;
    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assertion_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assertion_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assertion_failure("get of deleted record was not null");
    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assertion_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assertion_failure("wrong type thrown when add too big");
    }
    return true;
}

/**
 * Testing function for heap storage.
 * @return true if testing succeeded, false otherwise 
 */
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    cout << "create ok" << endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exsts ok" << endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    cout << "try insert" << endl;
    table.insert(&row);
    cout << "insert ok" << endl;
    cout << "try select" << endl;
    Handles* handles = table.select();
    cout << "select ok " << handles->size() << endl;
    cout << "try project" << endl;
    ValueDict *result = table.project((*handles)[0]);
    cout << "project ok" << endl;
    Value value = (*result)["a"];
    if (value.n != 12){
    	return false;
	}
    value = (*result)["b"];
    if (value.s != "Hello!"){
		return false;
	}
    table.drop();
    return true;
}

/*****************************************SlottedPage***************************************************************/

/**
 * Constructor for SlottedPage
 * @param block the block that holds all records
 * @param block_id the id for the block passed in
 * @param is_new indicates if the block passed in is a new one
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

/**
 * Add a new record to the block. Return its id.
 * @param data the record needed to be stored in block
 */
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

/**
 * Get a record by the record_id
 * @param record_id record ID
 */
Dbt* SlottedPage::get(RecordID record_id){
    u16 loc, size;
    get_header(size, loc, record_id);
    if(loc == 0){
        // tombstone
        return NULL;
    }
	//change based on lecture code
    Dbt* r = new Dbt(this->address(loc), size);
    return r;
}

/**
 * Update the record with the record_id to the record passed in
 * @param record_id record id
 * @param data the record
 */
void SlottedPage::put(RecordID record_id, const Dbt &data){
	u16 loc, size;
    get_header(size, loc, record_id);
    u16 new_size = data.get_size();
    if(new_size > size){
        u16 extra = new_size - size;
        if(!has_room(extra)){
            throw DbBlockNoRoomError("not enough room for new record");
        }
        slide(loc, loc-extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

/**
 * Delete a record
 * @param record_id record id
 */
void SlottedPage::del(RecordID record_id){
	u16 size, loc;
	get_header(size, loc, record_id);
	put_header(record_id, 0, 0);
	slide(loc, loc + size);
}

// Return all records
RecordIDs* SlottedPage::ids(void){
    RecordIDs *allIDs = new RecordIDs();
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

/**
 * Slide the record data in the block
 * @param start the old position of the block
 * @param end the new position of the block
 */
void SlottedPage::slide(u16 start, u16 end){
	u16 shift = end - start;
    if(shift == 0){
        return ;
    }

    // slide data
    void *to = this->address((u16)(this->end_free + 1 + shift));
    void *from = this->address((u16)(this->end_free + 1));
    u16 bytes = start - (this->end_free + 1);
    char temp[bytes];
    memcpy(temp, from, bytes);
    memcpy(to, temp, bytes);

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
    delete allIDs;
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

// Return the header information
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id){
	size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

// Return if there is available room in the block
bool SlottedPage::has_room(u16 size){
	u16 available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}


/*****************************************Heap File***************************************************************/
// Create a new Heapfile
void HeapFile::create(void){
    db_open(DB_CREATE|DB_EXCL);
    get_new();
}

// Drop a Heapfile physically
void HeapFile::drop(void){
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

// Open a Heapfile
void HeapFile::open(void){
    db_open();
}

// Close a Heapfile
void HeapFile::close(void){
    this->db.close(0);
    this->closed = true;
}

/**
 * Open the db
 * @param flags flags used in opening DB
 */
void HeapFile::db_open(uint flags) {
    if(!this->closed){
        return ;
    }
    try{
        this->db.set_re_len(DbBlock::BLOCK_SZ);
        this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);
    } catch(exception &e) {
        cerr << "db open failed: " << e.what() << endl;
    }
    DB_BTREE_STAT* stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = flags ? 0:stat->bt_ndata;
    this->closed = false;
}

/**
 * Allocate a new block for the database file.
 * Returns the new empty DbBlock that is managing the records in this block and its block id.
 */
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    BlockID block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

// Get a block by block_id
SlottedPage* HeapFile::get(BlockID block_id){
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

// Put a block into Heapfile
void HeapFile::put(DbBlock *block){
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    try{
        this->db.put(nullptr, &key, block->get_block(), 0);
    } catch(exception &e) {
        cerr << "db put block failed: " << e.what() << endl;
        exit(-1);
    } 
}

// Return all blocks
BlockIDs* HeapFile::block_ids(){
    BlockIDs *allIDs = new BlockIDs();
    for(BlockID cID = 1; cID <= this->last; cID ++){
        allIDs->push_back(cID);
    }
    return allIDs;
}


/*****************************************Heap Table***************************************************************/

/** 
 * @brief  Constructor for HeapTable that initializes variables including HeapFile
 * @param  Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : 
					DbRelation(table_name, column_names, column_attributes),
					file(table_name){
}

// Call create on file object HeapTable holds
void HeapTable::create(){
	this->file.create();
}

// Ok as create but tests if the object doesn't exist first
void HeapTable::create_if_not_exists(){
	try{
		this->file.open();
	}catch(DbException &e){
		create();
	}
}

// Calls the destructor on the HeapFile the HeapTable contains
void HeapTable::drop(){
	this->file.drop();
}

// Opens the HeapFile the HeapTable contains for insert, update, delete, select, and project methods
void HeapTable::open(){
	this->file.open();
}

// Closes the HeapFile the HeapTable contains, temporarily disabling insert, update, delete, select, and project methods
void HeapTable::close(){
	this->file.close();
}

/** @brief inserts a row into the table
    *  @param  ValueDict row (specifying the type and value)
    *  @return Handle to the record id and block id of insertion
    */
Handle HeapTable::insert(const ValueDict *row){
	this->open();
    ValueDict* validatedDict = validate(row);
    Handle handle = this->append(validatedDict);
    delete validatedDict;
    return handle;
}

/** @brief corresponds to the SQL query SELECT * FROM...WHERE. 
    *  @param  ValueDict representing a SQL WHERE clause
    *  @return Handles to the matching rows
    */
Handles* HeapTable::select(const ValueDict* where) {
    this->open();
    Handles* handles = new Handles();
    BlockIDs* block_ids = this->file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = this->file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/** @brief corresponds to the SQL query SELECT * FROM... 
    *  @return Handles to the matching rows
    */
Handles* HeapTable::select() {
    return select(nullptr);
}

/** @brief Check if the given row can be inserted 
    *  @param  ValueDict representing the row to be inserted
    *  @return rows that can be inserted
    */
ValueDict* HeapTable::validate(const ValueDict *row){
	ValueDict* full_row = new ValueDict();
	for(ValueDict::const_iterator itr = row->begin();itr != row->end();itr++){
		bool existence = false;
		for(ColumnAttribute columnAttri : this->column_attributes){
			if(itr->second.data_type == columnAttri.get_data_type()){
				existence = true;
                break;
			}
		}
		if(!existence){
			throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
		} else {
		    full_row->insert(pair<Identifier, Value>(itr->first,itr->second));
        }
	}
	return full_row;
}

/** @brief Assumes row is fully fleshed-out. Appends a record to the file. 
    *  @param  ValueDict representing a row
    *  @return Handles to the block and record id values where it was appended
    */
Handle HeapTable::append(const ValueDict *row){
	RecordID recId;
	BlockID lastBlockId = this->file.get_last_block_id();
	Dbt* data = marshal(row);
	SlottedPage* block = this->file.get(lastBlockId);
	try{
		recId = block->add(data);
	}catch(DbBlockNoRoomError &e){
		block = this->file.get_new();
        try{
		    recId = block->add(data);
            lastBlockId = block->get_block_id();
        } catch(DbBlockNoRoomError &e){
            cerr << "data is too large to be hold in one block" << endl;
            exit(-1);
        }
	}
    this->file.put(block);
    delete data;
    delete block;
    return Handle(lastBlockId, recId);
}

/**
 * Return the bits to go into the fil
 * caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
 * @param row the data needed to be marshal
 */
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
}

/**
 * Return the values from the block id and record id the handle paramater holds
 * returned value must be deallocated by caller
 * @param Handle holding the record id and block id of desired data
 */
ValueDict* HeapTable::project(Handle handle){
    ValueDict * row;
	Dbt* data;
	u32 blockId = handle.first;
	u16 recId = handle.second;
	SlottedPage * block = this->file.get(blockId);
	data = block->get(recId);
	row = unmarshal(data);
	delete block;
	delete data;
	return row;
}

// TODO
ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names){
    return NULL;
}

/**
 * Return the fields decoded from the bits
 * caller responsible for freeing the returned ValueDict
 * @param Dbt holding the bits representing the data
 */
ValueDict* HeapTable::unmarshal(Dbt *data){
    ValueDict* row = new ValueDict();
	char *block_bytes = (char*)data->get_data();
	uint offset = 0;
	uint col_num = 0;
	for (auto const& column_name: this->column_names) {
		ColumnAttribute ca = this->column_attributes[col_num++];
		Value value;
		if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
			value = Value(*(u32*) (block_bytes + offset));
			(*row)[column_name] = value;
			offset += 4;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = *(u16*) (block_bytes + offset);
			offset += sizeof(u16);
			char* s = new char[size + 1];
			memcpy(s, block_bytes+offset, size);
			s[size] = '\0';
			value = Value(s);
			(*row)[column_name] = value;
			offset += size;
			delete [] s;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
	}
	return row;
}

// TODO
void HeapTable::update(const Handle handle, const ValueDict *new_values){}

// TODO
void HeapTable::del(const Handle handle){}
