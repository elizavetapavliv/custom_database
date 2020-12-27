#pragma once
#include <map>
#include <vector>
#include "Connection.h"
#include "JsonComparator.h"
#include "Cursor.h"

namespace DatabaseLib
{
	class DATABASE_API Database
	{
	private:
		std::string META_FILE = "tables_meta.json";
		std::string TXT_EXT = ".txt";
		std::string INDEX_FILE = "_index.json";

		std::unordered_map<unsigned, std::unordered_map<std::string, Cursor>> connections;

		std::unordered_map<std::string, std::unordered_map<std::string, 
			std::map<json, std::vector<unsigned>, JsonComparator>>> tablesIndexes;

		json readJsonFromFile(std::string fileName);
		void loadIndex(std::string tableName);
		json readDataByOffset(std::string tableName, unsigned offset);
	public:
		Connection connect();
		void disconnect(Connection connection);
		void createTable(std::string tableName, json keysJson);
		void removeTable(std::string tableName);
		json getRowByKey(std::string tableName, json keyJson, Connection connection);
		json getRowInSortedTable(std::string tableName, std::string keyName,
			bool isReversed, Connection connection);
		json getNextRow(std::string tableName, Connection connection);
		json getPrevRow(std::string tableName, Connection connection);
	};
}