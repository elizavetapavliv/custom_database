#pragma once
#include <map>
#include <vector>
#include "Connection.h"
#include "JsonComparator.h"
#include "Cursor.h"
#include "DatabaseException.h"

namespace DatabaseLib
{
	class DATABASE_API Database
	{
	private:
		std::string META_FILE = "tables_meta.json";
		std::string TXT_EXT = ".txt";
		std::string JSON_EXT = ".json";

		std::unordered_map<unsigned, std::unordered_map<std::string, Cursor>> connections;

		std::unordered_map<std::string, std::unordered_map<std::string, Indexes>> tablesIndexes;

		json readJsonFromFile(std::string fileName);
		void loadIndex(std::string tableName, std::string keyName);
		json readDataByOffset(std::string tableName, unsigned offset);
		void ensureKeyIsFound(std::string tableName, std::string key);
		void ensureDataIsAvailable(Cursor cursor);
		void ensureIsConnected(Connection connection);
		void ensureTableExists(std::string tableName, json tablesMeta);
		void ensureTableIsNotEmpty(Indexes::iterator row, Indexes::iterator end);
		Cursor getCurrentCursor(std::string tableName, Connection connection);
	public:
		Connection connect();
		void disconnect(Connection connection);
		void createTable(std::string tableName, json keysJson, Connection connection);
		void removeTable(std::string tableName, Connection connection);
		json getRowByKey(std::string tableName, json keyJson, Connection connection);
		json getRowInSortedTable(std::string tableName, std::string keyName,
			bool isReversed, Connection connection);
		json getNextRow(std::string tableName, Connection connection);
		json getPrevRow(std::string tableName, Connection connection);
	};
}