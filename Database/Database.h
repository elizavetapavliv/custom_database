#pragma once
#include <unordered_map>
#include <vector>
#include "Connection.h"
#include "JsonComparator.h"

namespace DatabaseLib
{
	using Cursor = unsigned;

	class DATABASE_API Database
	{
	private:
		std::string META_FILE = "tables_meta.json";
		std::string JSON_EXT = ".json";
		std::string INDEX_FILE = "_index.json";
		std::unordered_map<unsigned, Cursor> connections;
		std::unordered_map<std::string, std::unordered_map<std::string, 
			std::map<json, std::vector<unsigned>, JsonComparator>>> tablesIndexes;
		json readJsonFromFile(std::string fileName);
		void loadIndex(std::string tableName);
		std::string readRowByOffset(std::string tableName, unsigned offset);
	public:
		Connection connect();
		void disconnect(Connection connection);
		void createTable(std::string tableName, std::string keysJson);
		void removeTable(std::string tableName);
		std::string getRowByKey(std::string tableName, std::string keyJson);
		std::string getRowInSortedTable(std::string tableName, std::string key, bool isReversed);
	};
}