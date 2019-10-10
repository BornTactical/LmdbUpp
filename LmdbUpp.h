#ifndef _LmdbUpp_LmdbUpp3_h_
#define _LmdbUpp_LmdbUpp3_h_

#include <LmdbUpp/lmdb.h>
#include <Core/Core.h>

namespace Upp {
	#define LMDB_EXCEPTION(Name) \
	struct Name : Exc { \
		Name(const char *s) : Exc(s) { \
			Cerr() << s << "\n"; \
		} \
	};
	
	LMDB_EXCEPTION(LmdbKeyExists)
	LMDB_EXCEPTION(LmdbNotFound)
	LMDB_EXCEPTION(LmdbPageNotFound)
	LMDB_EXCEPTION(LmdbPageMismatch)
	LMDB_EXCEPTION(LmdbPanic)
	LMDB_EXCEPTION(LmdbVersionMismatch)
	LMDB_EXCEPTION(LmdbInvalidFile)
	LMDB_EXCEPTION(LmdbMaxMapExceeded)
	LMDB_EXCEPTION(LmdbMaxDbsExceeded)
	LMDB_EXCEPTION(LmdbMaxReadersExceeded)
	LMDB_EXCEPTION(LmdbTLSLimitExceeded)
	LMDB_EXCEPTION(LmdbMaxDirtyTxnExceeded)
	LMDB_EXCEPTION(LmdbCursorDepthExceeded)
	LMDB_EXCEPTION(LmdbMapsizeTooSmall)
	LMDB_EXCEPTION(LmdbIncompatibleOptions)
	LMDB_EXCEPTION(LmdbInvalidReuse)
	LMDB_EXCEPTION(LmdbBadTransaction)
	LMDB_EXCEPTION(LmdbBadDatabase)
	LMDB_EXCEPTION(LmdbUnknownError)
	
	[[noreturn]]
	void LmdbRaiseError(int rc);
	
	constexpr uint64 operator"" _KB(uint64 kilobytes) { return kilobytes * 1024; }
	constexpr uint64 operator"" _MB(uint64 megabytes) { return megabytes * 1024 * 1024; }
	constexpr uint64 operator"" _GB(uint64 gigabytes) { return gigabytes * 1024 * 1024 * 1024; }
	
	template <typename...>
	using VoidT = void;
	
	template <typename AlwaysVoid, template <typename...> class Operation, typename... Args>
	struct DetectImpl : std::false_type {};
	
	template <template <typename...> class Operation, typename... Args>
	struct DetectImpl<VoidT<Operation<Args...>>, Operation, Args...> : std::true_type {};
	
	template <template <typename...> class Operation, typename... Args>
	using Detect = DetectImpl<VoidT<>, Operation, Args...>;
	
	template <typename T, typename MemFn>
	using HasSerialize = decltype(static_cast<MemFn>(&T::Serialize));
	
	template <typename T>
	using Serializable = Detect<HasSerialize, T, void(T::*)(Stream&)>;
	
	template <typename, typename, bool, size_t, unsigned int, MDB_dbi>
	class KeyValueStore;
	
	template<typename K = uint64, typename V = String>
	class Cursor {
		template <typename,typename,bool, size_t,unsigned int,MDB_dbi>
		friend class KeyValueStore;
		MDB_val first;
		MDB_val second;
	
	protected:
		MDB_cursor* cur  { nullptr };
		MDB_cursor_op op;
	
	public:
		K Key() {
			K key;
			
			if constexpr(std::is_integral<K>::value) {
				key = *(K*)(first.mv_data);
			}
			else if constexpr(std::is_same_v<K, String>) {
				key.Set((const char*)first.mv_data, first.mv_size);
			}
			
			return key;
		}
		
		V Value() {
			V value;
			
			if constexpr(std::is_same_v<V, String>) {
				value.Set((const char *)second.mv_data, second.mv_size);
			}
			else if constexpr(Serializable<V>::value) {
				MemReadStream ms(second.mv_data, second.mv_size);
				value.Serialize(ms);
			}
			
			return value;
		}
		
		void SeekBegin() {
			auto rc = mdb_cursor_get(cur, &first, &second, MDB_FIRST);
			if(rc == MDB_SUCCESS) return;
			LmdbRaiseError(rc);
		}
		
		void SeekEnd() {
			auto rc = mdb_cursor_get(cur, &first, &second, MDB_LAST);
			if(rc == MDB_SUCCESS) return;
			LmdbRaiseError(rc);
		}
		
		bool Next() {
			auto rc = mdb_cursor_get(cur, &first, &second, MDB_NEXT);
			if(rc == MDB_SUCCESS) return true;
			if(rc == MDB_NOTFOUND) return false;
			
			LmdbRaiseError(rc);
		}
		
		bool Prev() {
			auto rc = mdb_cursor_get(cur, &first, &second, MDB_PREV);
			if(rc == MDB_SUCCESS) return true;
			if(rc == MDB_NOTFOUND) return false;
			
			LmdbRaiseError(rc);
		}
		
		bool operator==(Cursor other) {
			return Key() == other.Key();
		}
		
		bool operator!=(Cursor other) {
			return Key() != other.Key();
		}
		
		~Cursor() {
			mdb_cursor_close(cur);
		}
	};
	
	template <
		typename K = uint64,
		typename V = String,
		bool AllowDuplicates = false,
		size_t       MaxMapSize = 64_MB,
		unsigned int MaxReaders = 125,
		MDB_dbi      MaxDBs     = 10
		>
	class KeyValueStore {
		static_assert(std::is_integral<K>::value || std::is_same_v<K, String>,
			"Key must be of an integral type or a String for now");
			
		static_assert(
			std::is_integral<V>::value ||
			std::is_same_v<V, String> ||
			Serializable<V>::value,
			"Value must be integral, String, POD, or have a Serialize method");
		
		MDB_env* env            { nullptr };
		MDB_txn* txn            { nullptr };
		MDB_dbi  dbi            { 0 };
		String   dbName         { Null };
		bool     isOpen         { false };
		
		unsigned int dbiFlags   { 0 };
		unsigned int putFlags   { 0 };
		unsigned int txnFlags   { 0 };
		
		void NewTransaction() {
			if constexpr(std::is_integral<K>::value) {
				dbiFlags |= MDB_INTEGERKEY;
				
				if constexpr(AllowDuplicates) {
					dbiFlags |= MDB_INTEGERDUP;
				}
			}
			else if constexpr(AllowDuplicates) {
				dbiFlags |= MDB_DUPSORT;
			}
			
			int rc = mdb_txn_begin(env, NULL, txnFlags, &txn);
			if(rc != MDB_SUCCESS) LmdbRaiseError(rc);

			rc = mdb_dbi_open(txn, IsNull(dbName) ? nullptr : dbName, dbiFlags, &dbi);
			if(rc == MDB_SUCCESS) { isOpen = true; return; }
			
			LmdbRaiseError(rc);
		}
		
		void CommitTransaction() {
			int rc = mdb_txn_commit(txn);
			if(rc == MDB_SUCCESS) { return; }
			
			LmdbRaiseError(rc);
		}
		
		void AbortTransaction() {
			mdb_txn_abort(txn);
			txn = nullptr;
		}
		
		void RenewTransaction() {
			int rc = mdb_txn_renew(txn);
			if(rc == MDB_SUCCESS) { return; }
			
			LmdbRaiseError(rc);
		}
		
		Cursor<K,V> CreateCursor() {
			Cursor<K,V> cur;
			auto rc = mdb_cursor_open(txn, dbi, &cur.cur);
			if(rc != MDB_SUCCESS) LmdbRaiseError(rc);
			
			return cur;
		}
		
	public:
		KeyValueStore& SetMaxReaders(int max) {
			auto rc = mdb_env_set_maxreaders(env, max);
			if(rc == MDB_SUCCESS) return *this;
			
			LmdbRaiseError(rc);
			
			return *this;
		}
		
		KeyValueStore& SetMaxMapSize(size_t max) {
			auto rc = mdb_env_set_mapsize (env, max);
			if(rc == MDB_SUCCESS) return *this;
			
			LmdbRaiseError(rc);
			
			return *this;
		}
		
		KeyValueStore& SetMaxDBs(int max) {
			auto rc = mdb_env_set_maxdbs(env, max);
			if(rc == MDB_SUCCESS) return *this;
			
			LmdbRaiseError(rc);
			
			return *this;
		}
		
		KeyValueStore& DbiFlags(unsigned int flags) {
			dbiFlags = flags;
			
			return *this;
		}
		
		bool IsEmpty() {
			MDB_stat stat;
			
			if(txn == nullptr) NewTransaction();
			mdb_stat(txn, dbi, &stat);
			
			return stat.ms_entries == 0;
		}
		
		void Commit() {
			CommitTransaction();
			txn = nullptr;
		}
		
		void Abort() {
			AbortTransaction();
			txn = nullptr;
		}
		
		Cursor<K,V> Begin() {
			if(txn == nullptr) NewTransaction();
			Cursor<K,V> cur = CreateCursor();
			
			auto rc = mdb_cursor_get(cur.cur, &(cur.first), &(cur.second), MDB_FIRST);
			if(rc == MDB_SUCCESS) return cur;
			
			LmdbRaiseError(rc);
		}
		
		Cursor<K,V> End() {
			if(txn == nullptr) NewTransaction();
			Cursor<K,V> cur = CreateCursor();
			
			auto rc = mdb_cursor_get(cur.cur, &(cur.first), &(cur.second), MDB_LAST);
			if(rc == MDB_SUCCESS) return cur;
			LmdbRaiseError(rc);
		}
		
		Cursor<K,V> FindFirst(K key) {
			if(txn == nullptr) NewTransaction();
			Cursor<K,V> cur = CreateCursor();
			
			if constexpr(std::is_integral<K>::value) {
				cur.first = { sizeof(K), static_cast<void*>(&key) };
			}
			else if constexpr(std::is_same_v<K, String>) {
				cur.first = { key.GetLength(), (void*)key.begin() };
			}
			auto rc = mdb_cursor_get(cur.cur, &(cur.first), &(cur.second), MDB_SET_KEY);
			if(rc == MDB_SUCCESS) return cur;

			LmdbRaiseError(rc);
		}
		
		void Get(K key, V& value) {
			if(txn == nullptr) NewTransaction();
			MDB_val first;
			
			if constexpr(std::is_integral<K>::value) {
				first.mv_size = sizeof(K);
				first.mv_data = &key;
			}
			else if constexpr(std::is_same_v<K, String>) {
				first.mv_size = key.GetLength();
				first.mv_data = (void *)key.Begin();
			}
			
			MDB_val second;
			
			auto rc = mdb_get(txn, dbi, &first, &second);
			if(rc == MDB_SUCCESS) {
				if constexpr(std::is_same_v<V, String>) {
					value.Set((const char *)second.mv_data, second.mv_size);
				}
				else if constexpr(Serializable<V>::value) {
					MemReadStream ms(second.mv_data, second.mv_size);
					value.Serialize(ms);
					AbortTransaction();
				}
				
				return;
			}
			
			LmdbRaiseError(rc);
		}
		
		void Add(K key, V& value, bool autocommit = true) {
			if(txn == nullptr) NewTransaction();
			
			MDB_val first;
			
			if constexpr(std::is_integral<K>::value) {
				first.mv_size = sizeof(K);
				first.mv_data = &key;
			}
			else if constexpr(std::is_same_v<K, String>) {
				first.mv_size = key.GetLength();
				first.mv_data = (void *)key.Begin();
			}
			
			MDB_val second;
			
			if constexpr(std::is_same_v<V, String>) {
				second.mv_size = (size_t)value.GetLength();
				second.mv_data = (void*)value.Begin();
				
				auto rc = mdb_put(txn, dbi, &first, &second, putFlags);
				if(rc != MDB_SUCCESS) LmdbRaiseError(rc);
			}
			else if constexpr(Serializable<V>::value) {
				StringStream ss;
				ss.SetStoring();
				value.Serialize(ss);
				
				String str = ss.GetResult();
				
				second.mv_size = (size_t)str.GetLength();
				second.mv_data = (void *)str.Begin();
				
				auto rc = mdb_put(txn, dbi, &first, &second, putFlags);
				if(rc != MDB_SUCCESS) LmdbRaiseError(rc);
			}
			
			if(autocommit) {
				CommitTransaction();
			}
		}
		
		KeyValueStore(String directory, String dbName = Null) : dbName(dbName) {
			if(!IsNull(dbName)) {
				dbiFlags = MDB_CREATE;
			}
			
			if(!DirectoryExists(directory)) DirectoryCreate(directory, 0775);

			auto rc = mdb_env_create(&env);
			if(rc != MDB_SUCCESS) LmdbRaiseError(rc);
			
			SetMaxMapSize(MaxMapSize);
			SetMaxReaders(MaxReaders);
			SetMaxDBs(MaxDBs);

			rc = mdb_env_open(env, directory, 0, 0775);
			if(rc != MDB_SUCCESS) LmdbRaiseError(rc);
			
			isOpen = true;
		}
		
		~KeyValueStore() {
			mdb_close(env, dbi);
			mdb_env_close(env);
		}
	};
}

#endif
