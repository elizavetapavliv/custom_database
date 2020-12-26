#include "pch.h"
#include "Database.h"
#include <fstream>
#include <sstream>

namespace DatabaseLib
{
	Connection Database::connect()
	{
		Connection connection = Connection();
		connections[connection.connectionId] = 0;
		return connection;
	}

	void Database::disconnect(Connection connection) 
	{
		connections.erase(connection.connectionId);
	}

	json Database::readJsonFromFile(std::string fileName) 
	{
		json result = json::object();
		std::ifstream file (fileName);

		if (file.is_open())
		{
			std::stringstream fileContent;
			fileContent << file.rdbuf();
			result = json::parse(fileContent.str());
		}
		return result;
	}

	void Database::createTable(std::string tableName, std::string keysJson)
	{
		json tablesMeta = readJsonFromFile(META_FILE);
		tablesMeta[tableName]["keys"] = json::parse(keysJson);

		std::ofstream tablesMetaFile(META_FILE);
		tablesMetaFile << tablesMeta.dump(4);
	}

	void Database::removeTable(std::string tableName)
	{
		json tablesMeta = readJsonFromFile(META_FILE);
		tablesMeta.erase(tableName);

		std::ofstream tableFile(META_FILE);
		tableFile << tablesMeta.dump(4);

		remove((tableName + JSON_EXT).c_str());
		remove((tableName + INDEX_FILE).c_str());
	}

	std::string Database::getRowByKey(std::string tableName, std::string keyJson)
	{
		loadIndex(tableName);

		json key = json::parse(keyJson);
		auto properties = key.items().begin();
		unsigned offset = tablesIndexes[tableName][properties.key()][properties.value()][0];

		return readRowByOffset(tableName, offset);
	}

	std::string Database::getRowInSortedTable(std::string tableName, std::string key, bool isReversed)
	{
		loadIndex(tableName);

		auto keyValues = tablesIndexes[tableName][key];
		unsigned offset = 0u;
		if (isReversed) 
		{
			auto it = keyValues.rbegin();
			if (it != keyValues.rend())
			{
				offset = it->second[0];
			}
		}
		else
		{
			auto it = keyValues.begin();
			if (it != keyValues.end())
			{
				offset = it->second[0];
			}
		}
		return readRowByOffset(tableName, offset);
	}

	std::string Database::readRowByOffset(std::string tableName, unsigned offset)	
	{
		std::ifstream tableFile(tableName + JSON_EXT);
		tableFile.seekg(offset, std::ios::beg);
		std::string value;
		std::getline(tableFile, value);
		return value;
	}

	void Database::loadIndex(std::string tableName) 
	{
		if (tablesIndexes.find(tableName) == tablesIndexes.end())
		{
			std::ifstream tableIndexFile(tableName + INDEX_FILE);
			std::stringstream fileContent;
			fileContent << tableIndexFile.rdbuf();
			json indexes = json::parse(fileContent.str());
			std::unordered_map<std::string, std::map<json, std::vector<unsigned>, JsonComparator>> keysMap;

			for (auto& index : indexes)
			{
				std::vector<unsigned> offsets = index["offsets"].get<std::vector<unsigned>>();
				json keys = index["keys"];
				for (auto& keyValue : keys.items())
				{
					keysMap[keyValue.key()][keyValue.value()] = offsets;
				}
			}
			tablesIndexes[tableName] = keysMap;
		}
	}
}