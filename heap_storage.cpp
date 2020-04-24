#include "heap_storage.h"
#include <cstring>
#include <exception>
#include <map>
#include <utility>
#include <vector>
using namespace std;


typedef u_int16_t u16;
typedef u_int32_t u32;


bool test_slottedpage(){
    bool same = true;

    char block[DbBlock::BLOCK_SZ];
    Dbt dbtBlock(block, DbBlock::BLOCK_SZ);
    SlottedPage page(dbtBlock, 1, true);

    // add the first record "hello world1" into DB
    char rec1[] = "hello world1";
    Dbt dbtRec1(rec1, sizeof(rec1));
    RecordID recID = page.add(&dbtRec1);
    Dbt *record = page.get(recID);
    if(strcmp((char*)record->get_data(), rec1) != 0){
        same = false;
        cerr << "From DB:" << (char*)record->get_data() << "/" << "Raw Data:" << rec1 << endl;
    }

    // add the second record "hello world(2)" into DB
    char rec2[] = "hello world(2)";
    Dbt dbtRec2(rec2, sizeof(rec2));
    recID = page.add(&dbtRec2);
    RecordID rec2ID = recID;
    record = page.get(recID);
    if(strcmp((char*)record->get_data(), rec2) != 0){
        same = false;
        cerr << "From DB:" << (char*)record->get_data() << "/" << "Raw Data:" << rec2 << endl;
    }

    // add the third record "hello world, I'm 3rd" into DB
    char rec3[] = "hello world, I'm 3rd";
    Dbt dbtRec3(rec3, sizeof(rec3));
    recID = page.add(&dbtRec3);
    record = page.get(recID);
    if(strcmp((char*)record->get_data(), rec3) != 0){
        same = false;
        cerr << "From DB:" << (char*)record->get_data() << "/" << "Raw Data:" << rec3 << endl;
    }
    
    // update the second record to "hello 2"
    char newRec1[] = "hello 2";
    Dbt newDbtRec1(newRec1, sizeof(newRec1));
    page.put(rec2ID, newDbtRec1);
    record = page.get(rec2ID);
    if(strcmp((char*)record->get_data(), newRec1) != 0){
        same = false;
        cerr << "From DB:" << (char*)record->get_data() << "/" << "Raw Data:" << newRec1 << endl;
    }
    
    // update the second record to "hello world(2), I have more data"
    char newRec2[] = "hello world(2), I have more data";
    Dbt newDbtRec2(newRec2, sizeof(newRec2));
    page.put(rec2ID, newDbtRec2);
    record = page.get(rec2ID);
    if(strcmp((char*)record->get_data(), newRec2) != 0){
        same = false;
        cerr << "From DB:" << (char*)record->get_data() << "/" << "Raw Data:" << newRec2 << endl;
    }

    return same;
}

bool test_heapfile(){
    HeapFile* heapfile = new HeapFile("test");
    bool same = true;

    heapfile->create();
    if(heapfile->get_last_block_id() != 1){
        same = false;
    }

    for(int i = 1; i <= 9; i ++){
        heapfile->get_new();
    }

    if(heapfile->get_last_block_id() != 10){
        same = false;
    }

    SlottedPage *page = heapfile->get(5U);
    heapfile->put(page);
    if(heapfile->get_last_block_id() != 10){
        same = false;
    }

    heapfile->drop();

    return same;
}

bool test_heap_storage() {
	cout << "hello test" << endl;
	return true;
}

/*****************************************SlottedPage***************************************************************/


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
	//change based on lecture code
    Dbt* r = new Dbt(this->address(loc), size);
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
        slide(loc, loc-extra);
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

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id){
	size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}


bool SlottedPage::has_room(u16 size){
	u16 available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}


/*****************************************Heap File***************************************************************/

void HeapFile::create(void){
    db_open(DB_CREATE|DB_EXCL);
    get_new();
}

void HeapFile::drop(void){
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

void HeapFile::open(void){
    db_open();
}

void HeapFile::close(void){
    if(this->closed){
        return ;
    }
    this->closed = true;
    this->db.close(0);
}

void HeapFile::db_open(uint flags) {
    if(!this->closed){
        return ;
    }
    try{
        this->db.set_re_len(DbBlock::BLOCK_SZ);
        this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);
    } catch(exception &e) {
        cerr << "db open failed: " << e.what() << endl;
        exit(-1);
    }
    BlockIDs *allIDs = block_ids();
    this->last = flags ? 0:allIDs->size();
    this->closed = false;
    delete allIDs;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
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

SlottedPage* HeapFile::get(BlockID block_id){
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

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

BlockIDs* HeapFile::block_ids(){
    BlockIDs *allIDs = new BlockIDs();
    for(BlockID cID = 1; cID <= this->last; cID ++){
        allIDs->push_back(cID);
    }
    return allIDs;
}


/*****************************************Heap Table***************************************************************/

/** @brief  Constructor for HeapTable that initializes variables including HeapFile
 *  @param  Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : 
					DbRelation(table_name, column_names, column_attributes),
					file(table_name){
}

/** @brief Call create on file object HeapTable holds
    */
void HeapTable::create(){
	this->file.create();
}

/** @brief same as create but tests if the object doesn't exist first
    */
void HeapTable::create_if_not_exists(){
	try{
		this->file.open();
	}catch(DbException &e){
		create();
	}
}

/** @brief Calls the destructor on the HeapFile the HeapTable contains
    */
void HeapTable::drop(){
	this->file.~HeapFile();
}

/** Opens the HeapFile the HeapTable contains for insert, 
  * update, delete, select, and project methods
    */
void HeapTable::open(){
	this->file.open();
}

/** Closes the HeapFile the HeapTable contains, temporarily disabling insert, 
  * update, delete, select, and project methods
    */
void HeapTable::close(){
	this->file.close();
}

/** @brief inserts a row into the table
    *  @param  ValueDict row (specifying the type and value)
    *  @return Handle to the record id and block id of insertion
    */
Handle HeapTable::insert(const ValueDict *row){
	open();
	ValueDict* validatedDict = validate(row);

	return append(validatedDict);
}

/** @brief corresponds to the SQL query SELECT * FROM...WHERE. 
    *  @param  ValueDict representing a SQL WHERE clause
    *  @return Handles to the matching rows
    */
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

/** @brief Check if the given row can be inserted 
    *  @param  ValueDict representing the row to be inserted
    *  @return rows that can be inserted
    */
ValueDict* HeapTable::validate(const ValueDict *row){
	ValueDict* full_row = new ValueDict();
	//for(int i = 0;i < (int)column_attributes.size();i++){
	/*for(ColumnAttribute columnAttri : this->column_attributes){
		Value val;
		ColumnAttribute colName = columnAttri;
		if(row->find(columnAttri) != 5){
			throw "don't know how to handle NULLs, defaults, etc. yet";
			return nullptr;
		}
		val = Value(row[columnAttri]);
		
		full_row[columnAttri] = val;
	}*/
	ValueDict::const_iterator itr;
	for(itr = row->begin();itr != row->end();itr++){
		itr->first;
		bool existence = false;
		for(ColumnAttribute columnAttri : this->column_attributes){
			if(itr->second.data_type == columnAttri.get_data_type()){
				existence = true;
			}
		}
		if(!existence){
			throw "don't know how to handle NULLs, defaults, etc. yet";
			return nullptr;
		}
		full_row->insert(pair<Identifier, Value>(itr->first,itr->second));
	}
	return full_row; 
}

/** @brief Assumes row is fully fleshed-out. Appends a record to the file. 
    *  @param  ValueDict representing a row
    *  @return Handles to the block and record id values where it was appended
    */
Handle HeapTable::append(const ValueDict *row){
	RecordID recId;
	Handle handle;
	BlockID lastBlockId = this->file.get_last_block_id();
	Dbt *data = marshal(row);
	SlottedPage * block = this->file.get(lastBlockId);
	try{
		recId = block->add(data);
	}catch(DbBlockNoRoomError &e){
		block = this->file.get_new();
		recId = block->add(data);
	}
	
	
	/*DbBlock* db_block = (DbBlock*)data->get_data();
	this->file.put(db_block);
	delete db_block;*/
	this->file.put(block);
	delete data;
	//this might be a bad delete (the block delete)
	delete block;
	
	handle.first = lastBlockId;
	handle.second = recId;
	return handle;
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
}

// TODO
Handles* HeapTable::select(){
    return NULL;
}

// TODO
ValueDict* HeapTable::project(Handle handle){
    return NULL;
}

// TODO
ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names){
    return NULL;
}

// TODO
ValueDict* HeapTable::unmarshal(Dbt *data){
    return NULL;
}

// TODO
void HeapTable::update(const Handle handle, const ValueDict *new_values){}

// TODO
void HeapTable::del(const Handle handle){}