#include "pch.h"
#include "Database.h"
#include <fstream>
#include <sstream>

namespace DatabaseLib
{
	Connection Database::connect()
	{
		Connection connection = Connection();
		connections[connection.getConnectionId()];
		return connection;
	}

	void Database::disconnect(Connection connection) 
	{
		if (!connections.erase(connection.getConnectionId()))
		{
			throw DatabaseException("You havent't been connected", ErrorCode::NO_CONNECTION);
		}
	}

	void Database::createTable(std::string tableName, json keysJson, Connection connection)
	{
		checkConnection(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		tablesMeta = tablesMeta.is_null() ? json::object() : tablesMeta;
 		tablesMeta[tableName]["keys"] = keysJson;

		for (auto key : keysJson)
		{
			std::ofstream tableIndexFile(tableName + "_" + key.dump() + JSON_EXT);
			tableIndexFile << json::array().dump();
		}

		std::ofstream tablesMetaFile(META_FILE);
		tablesMetaFile << tablesMeta.dump();
	}

	void Database::removeTable(std::string tableName, Connection connection)
	{
		checkConnection(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		checkIfTableExists(tableName, tablesMeta);

		json keysJson = tablesMeta[tableName]["keys"];
		for (auto key : keysJson)
		{
			remove((tableName + "_" + key.dump() + JSON_EXT).c_str());
		}

		tablesMeta.erase(tableName);
		std::ofstream tableFile(META_FILE);
		tableFile << tablesMeta.dump();

		remove((tableName + TXT_EXT).c_str());
	}

	json Database::getRowByKey(std::string tableName, json keyJson, Connection connection)
	{
		checkConnection(connection);
		
		auto properties = keyJson.items().begin();
		std::string keyName = properties.key();

		loadIndex(tableName, keyName);

		auto row = tablesIndexes[tableName][keyName].find(properties.value());
		auto end = tablesIndexes[tableName][keyName].end();
		if (row == end)
		{
			throw DatabaseException("Key value not found", ErrorCode::KEY_VALUE_NOT_FOUND);
		}

		unsigned offset = row->second[0];
		Cursor currentRow (row, end, 0, keyName);
		connections[connection.getConnectionId()][tableName] = currentRow;

		return readDataByOffset(tableName, offset);
	}

	json Database::getRowInSortedTable(std::string tableName, std::string keyName, 
		bool isReversed, Connection connection)
	{
		checkConnection(connection);
		loadIndex(tableName, keyName);

		Indexes::iterator row;
		int offsetIndex;

		if (isReversed)
		{
			row = --tablesIndexes[tableName][keyName].end();
			offsetIndex = row->second.size() - 1;
		}
		else 
		{
			row = tablesIndexes[tableName][keyName].begin();
			offsetIndex = 0;
		}
		unsigned offset = row->second[offsetIndex];

		Cursor currentRow(row, tablesIndexes[tableName][keyName].end(), offsetIndex, keyName);
		connections[connection.getConnectionId()][tableName] = currentRow;

		return readDataByOffset(tableName, offset);
	}

	json Database::getNextRow(std::string tableName, Connection connection)
	{
		Cursor cursor = getCurrentCursor(tableName, connection);

		if ((unsigned)cursor.offsetIndex >= cursor.currentRow->second.size() - 1)
		{
			cursor.currentRow++;
			checkIfDataIsAvailable(cursor);
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
		Cursor cursor = getCurrentCursor(tableName, connection);

		if (cursor.offsetIndex == 0)
		{
			cursor.currentRow--;
			checkIfDataIsAvailable(cursor);
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

	json Database::readJsonFromFile(std::string fileName)
	{
		json result;
		std::ifstream file(fileName);

		if (file.is_open())
		{
			std::stringstream fileContent;
			fileContent << file.rdbuf();
			result = json::parse(fileContent.str());
		}
		return result;
	}

	json Database::readDataByOffset(std::string tableName, unsigned offset)	
	{
		std::ifstream tableFile(tableName + TXT_EXT);
		tableFile.seekg(offset, std::ios::beg);
		std::string value;
		std::getline(tableFile, value);
		return json::parse(value);
	}

	void Database::loadIndex(std::string tableName, std::string keyName)
	{
		if (tablesIndexes.find(tableName) == tablesIndexes.end() || 
			tablesIndexes[tableName].find(keyName) == tablesIndexes[tableName].end())
		{
			json indexes = readJsonFromFile(tableName + "_" + keyName + JSON_EXT);
			if (indexes.is_null())
			{
				throw DatabaseException("Table or key not found: " + tableName + ", " + keyName, ErrorCode::NOT_FOUND);
			}
			std::unordered_map<std::string, std::map<json, std::vector<unsigned>, JsonComparator>> keysMap;

			for (auto& index : indexes)
			{
				auto keyValue = index.items().begin();
				keysMap[keyValue.key()][keyValue.value()] = index["offsets"].get<std::vector<unsigned>>();		
			}
			tablesIndexes[tableName] = keysMap;
		}
	}

	void Database::checkIfKeyIsFound(std::string tableName, std::string key)
	{
		if (tablesIndexes[tableName].find(key) == tablesIndexes[tableName].end())
		{
			throw DatabaseException("Key not found: " + key , ErrorCode::KEY_NOT_FOUND);
		}
	}

	void Database::checkIfDataIsAvailable(Cursor cursor)
	{
		if (cursor.currentRow == cursor.end)
		{
			throw DatabaseException("No more data available", ErrorCode::NO_MORE_DATA_AVAILABLE);
		}
	}

	void Database::checkConnection(Connection connection)
	{
		if (connections.find(connection.getConnectionId()) == connections.end())
		{
			throw DatabaseException("You havent't been connected", ErrorCode::NO_CONNECTION);
		}
	}

	void Database::checkIfTableExists(std::string tableName, json tablesMeta)
	{
		if (tablesMeta.is_null() || tablesMeta.empty() || !tablesMeta.contains(tableName))
		{
			throw DatabaseException("Table not found: " + tableName, ErrorCode::TABLE_NOT_FOUND);
		}
	}

	Cursor Database::getCurrentCursor(std::string tableName, Connection connection)
	{
		checkConnection(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		checkIfTableExists(tableName, tablesMeta);
		
		Cursor cursor = connections[connection.getConnectionId()][tableName];
		if (cursor.offsetIndex == -1)
		{
			throw DatabaseException("Cursor wasn't opened", ErrorCode::CURSOR_NOT_OPENED);
		}
		loadIndex(tableName, cursor.keyName);

		return cursor;
	}
}