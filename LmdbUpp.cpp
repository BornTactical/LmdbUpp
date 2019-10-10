#include "LmdbUpp.h"

namespace Upp {
	[[noreturn]]
	void LmdbRaiseError(int rc) {
		switch(rc) {
			case MDB_KEYEXIST:
				throw LmdbKeyExists("Error: Key already exists in database.");
				break;
			case MDB_NOTFOUND:
				throw LmdbNotFound("Error: Key/Value Pair not found in database.");
				break;
			case MDB_PAGE_NOTFOUND:
				throw LmdbPageNotFound("Error: Page not found in database. Corruption likely");
				break;
			case MDB_CORRUPTED:
				throw LmdbPageMismatch("Error: Located page was of wrong type.");
				break;
			case MDB_PANIC:
				throw LmdbPanic("Error: Update of meta page failed or environment had fatal error.");
				break;
			case MDB_VERSION_MISMATCH:
				throw LmdbVersionMismatch("Error: DbEnv is incompatible version.");
				break;
			case MDB_INVALID:
				throw LmdbInvalidFile("Error: File is not a valid LMDB file.");
				break;
			case MDB_MAP_FULL:
				throw LmdbMaxMapExceeded("Error: Maximum mapsize exceeded. Consider increasing MaxMapSize.");
				break;
			case MDB_DBS_FULL:
				throw LmdbMaxDbsExceeded("Error: Maximum database count exceeded. Consider increasing MaxDBs.");
				break;
			case MDB_READERS_FULL:
				throw LmdbMaxReadersExceeded("Error: Maximum reader count exceed. Consider increasing MaxReaders.");
				break;
			case MDB_TLS_FULL:
				throw LmdbTLSLimitExceeded("Error: Too many keys are being stored in thread local storage.");
				break;
			case MDB_TXN_FULL:
				throw LmdbMaxDirtyTxnExceeded("Error: Transaction has too many dirty pages");
				break;
			case MDB_CURSOR_FULL:
				throw LmdbCursorDepthExceeded("Error: Cursor stack has exceeded its maximum depth");
				break;
			case MDB_MAP_RESIZED:
				throw LmdbMapsizeTooSmall("Error: The database is bigger than MaxMapSize");
				break;
			case MDB_INCOMPATIBLE:
				throw LmdbIncompatibleOptions("Error: Operation requested is incompatible with DbiOptions. Ex. running a write transaction on a read only database.");
				break;
			case MDB_BAD_RSLOT:
				throw LmdbInvalidReuse("Error: The reader locktable slot cannot be reused this way.");
				break;
			case MDB_BAD_TXN:
				throw LmdbBadTransaction("Error: Transaction must abort, has a child, or is invalid.");
				break;
			case MDB_BAD_DBI:
				throw LmdbBadDatabase("Error: Database was changed unexpectedly.");
				break;
			case EACCES:
				throw LmdbUnknownError("Error: An attempt was made to write in a read-only transaction.");
				break;
			case EINVAL:
				throw LmdbUnknownError("Error: an invalid parameter was specified.");
				break;
			case MDB_PROBLEM:
				throw LmdbUnknownError("Error: Unexpected problem, transaction should abort.");
				break;
			default:
				throw LmdbUnknownError("Error: Unknown error");
				break;
		}
	}
	//$+
}