#include "pch.h"
#include "Database.h"
#include <fstream>
#include <sstream>

namespace DatabaseLib
{
	Connection Database::connect()
	{
		return Connection();
	}

	void Database::disconnect(Connection connection) 
	{
		connections.erase(connection.getConnectionId());
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

	void Database::createTable(std::string tableName, json keysJson)
	{
		json tablesMeta = readJsonFromFile(META_FILE);
		tablesMeta[tableName]["keys"] = keysJson;

		std::ofstream tablesMetaFile(META_FILE);
		tablesMetaFile << tablesMeta.dump();
	}

	void Database::removeTable(std::string tableName)
	{
		json tablesMeta = readJsonFromFile(META_FILE);
		tablesMeta.erase(tableName);

		std::ofstream tableFile(META_FILE);
		tableFile << tablesMeta.dump();

		remove((tableName + TXT_EXT).c_str());
		remove((tableName + INDEX_FILE).c_str());
	}

	json Database::getRowByKey(std::string tableName, json keyJson, Connection connection)
	{
		loadIndex(tableName);

		auto properties = keyJson.items().begin();
		std::string keyName = properties.key();

		auto row = tablesIndexes[tableName][keyName].find(properties.value());
		unsigned offset = 0u;

		if (row != tablesIndexes[tableName][keyName].end())
		{
			offset = row->second[0];
		}

		Cursor currentRow (row, 0u);
		connections[connection.getConnectionId()][tableName] = currentRow;

		return readDataByOffset(tableName, offset);
	}

	json Database::getRowInSortedTable(std::string tableName, std::string keyName, 
		bool isReversed, Connection connection)
	{
		loadIndex(tableName);

		std::map<json, std::vector<unsigned>, JsonComparator>::iterator row;
		unsigned offsetIndex;

		if (isReversed)
		{
			row = --tablesIndexes[tableName][keyName].end();
			offsetIndex = row->second.size() - 1;
		}
		else 
		{
			row = tablesIndexes[tableName][keyName].begin();
			offsetIndex = 0u;
		}
		unsigned offset = row->second[offsetIndex];

		Cursor currentRow(row, offsetIndex);
		connections[connection.getConnectionId()][tableName] = currentRow;

		return readDataByOffset(tableName, offset);
	}

	json Database::getNextRow(std::string tableName, Connection connection)
	{
		Cursor cursor = connections[connection.getConnectionId()][tableName];
		if (cursor.offsetIndex >= cursor.currentRow->second.size() - 1)
		{
			cursor.currentRow++;
			cursor.offsetIndex = 0u;
		}
		else
		{
			cursor.offsetIndex++;
		}
		connections[connection.getConnectionId()][tableName] = cursor;
		unsigned offset = (cursor.currentRow->second)[cursor.offsetIndex];

		return readDataByOffset(tableName, offset);
	}

	json Database::getPrevRow(std::string tableName, Connection connection)
	{
		Cursor cursor = connections[connection.getConnectionId()][tableName];
		if (cursor.offsetIndex == 0)
		{
			cursor.currentRow--;
			auto offsets = cursor.currentRow->second;
			cursor.offsetIndex = offsets.size() - 1;
		}
		else
		{
			cursor.offsetIndex--;
		}		
		connections[connection.getConnectionId()][tableName] = cursor;
		unsigned offset = cursor.currentRow->second[cursor.offsetIndex];

		return readDataByOffset(tableName, offset);
	}

	json Database::readDataByOffset(std::string tableName, unsigned offset)	
	{
		std::ifstream tableFile(tableName + TXT_EXT);
		tableFile.seekg(offset, std::ios::beg);
		std::string value;
		std::getline(tableFile, value);
		return json::parse(value);
	}

	void Database::loadIndex(std::string tableName) 
	{
		if (tablesIndexes.find(tableName) == tablesIndexes.end())
		{
			json indexes = readJsonFromFile(tableName + INDEX_FILE);
			std::unordered_map<std::string, std::map<json, std::vector<unsigned>, JsonComparator>> keysMap;

			for (auto& index : indexes)
			{
				auto keyValue = index.items().begin();
				keysMap[keyValue.key()][keyValue.value()] = index["offsets"].get<std::vector<unsigned>>();
			
			}
			tablesIndexes[tableName] = keysMap;
		}
	}
}