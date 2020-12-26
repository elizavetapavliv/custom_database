#pragma once
#include "Connection.h"
#include <unordered_map>
#include <vector>
#include "nlohmann/json.hpp"

namespace DatabaseLib
{
	using Cursor = unsigned;
	using json = nlohmann::json;

	class DATABASE_API Database
	{
	private:
		std::string META_FILE = "tables_meta.json";
		std::string JSON_EXT = ".json";
		std::string INDEX_FILE = "_index.json";
		std::unordered_map<unsigned, Cursor> connections;
		std::unordered_map<std::string, std::unordered_map<std::string, std::map<std::string, std::vector<unsigned>>>> tablesIndexes;
		json readJsonFromFile(std::string fileName);
		void loadIndex(std::string tableName);
		std::string readRowByOffset(std::string tableName, unsigned offset);
	public:
		Connection connect();
		void disconnect(Connection connection);
		void createTable(std::string tableName, std::string keysJson);
		void removeTable(std::string tableName);
		std::string getRowByKey(std::string tableName, std::string keyJson);
		std::string getRowInSortedTable(std::string tableName, std::string key);
	};
}