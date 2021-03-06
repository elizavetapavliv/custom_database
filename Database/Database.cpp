#include "pch.h"
#include "Database.h"
#include <fstream>
#include <sstream>
#include <mutex>

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
		std::unique_lock lock(mutex_);
		ensureIsConnected(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		tablesMeta = tablesMeta.is_null() ? json::object() : tablesMeta;
 		tablesMeta[tableName]["keys"] = keysJson;

		for (auto key : keysJson.items())
		{
			std::ofstream tableIndexFile(tableName + "_" + key.key() + JSON_EXT);
			tableIndexFile << json::array().dump();
		}

		std::ofstream tablesMetaFile(META_FILE);
		tablesMetaFile << tablesMeta.dump();
	}

	void Database::removeTable(std::string tableName, Connection connection)
	{
		std::unique_lock lock(mutex_);
		ensureIsConnected(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		ensureTableExists(tableName, tablesMeta);

		json keysJson = tablesMeta[tableName]["keys"];
		for (auto key : keysJson.items())
		{
			remove((tableName + "_" + key.key() + JSON_EXT).c_str());
		}

		tablesMeta.erase(tableName);
		std::ofstream tableFile(META_FILE);
		tableFile << tablesMeta.dump();

		remove((tableName + TXT_EXT).c_str());
	}

	void Database::addKey(std::string tableName, json keysJson, Connection connection)
	{
		std::unique_lock lock(mutex_);
		ensureIsConnected(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		auto newKey = keysJson.items().begin();
		std::string keyName = newKey.key();
		tablesMeta[tableName]["keys"][keyName] = newKey.value();

		std::ifstream tableFileIn(tableName + TXT_EXT);
		std::string value;
		auto pos = tableFileIn.tellg();
		while (std::getline(tableFileIn, value))
		{
			json entry = json::parse(value), key;
			for (std::string keyColumn : newKey.value())
			{
				key[keyColumn] = entry[keyColumn];
			}
			auto curr = tablesIndexes[tableName][keyName].find(key);
			auto end = tablesIndexes[tableName][keyName].end();
			if (curr == end)
			{
				tablesIndexes[tableName][keyName][key] = { (unsigned)pos };
			}
			else
			{
				curr->second.push_back((unsigned)pos);
			}
			pos = tableFileIn.tellg();
		}
		tableFileIn.close();

		dumpIndex(tableName, keyName);

		std::ofstream tablesMetaFile(META_FILE);
		tablesMetaFile << tablesMeta.dump();
	}

	void Database::removeKey(std::string tableName, std::string keyName, Connection connection)
	{
		std::unique_lock lock(mutex_);
		ensureIsConnected(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		tablesMeta[tableName]["keys"].erase(keyName);
		std::ofstream tablesMetaFile(META_FILE);
		tablesMetaFile << tablesMeta.dump();

		tablesIndexes[tableName].erase(keyName);

		std::remove((tableName + "_" + keyName + JSON_EXT).c_str());
	}

	json Database::getRowByKey(std::string tableName, json keyJson, Connection connection)
	{
		ensureIsConnected(connection);
		auto properties = keyJson.items().begin();
		std::string keyName = properties.key();
		{
			std::unique_lock lock(mutex_);
			loadIndex(tableName, keyName);
		}

		std::shared_lock lock(mutex_);

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
		ensureIsConnected(connection);
		{
			std::unique_lock lock(mutex_);
			loadIndex(tableName, keyName);
		}
		std::shared_lock lock(mutex_);

		Indexes::iterator row;
		int offsetIndex;

		if (isReversed)
		{
			row = --tablesIndexes[tableName][keyName].end();
			ensureTableIsNotEmpty(row, tablesIndexes[tableName][keyName].end());
			offsetIndex = row->second.size() - 1;
		}
		else 
		{
			row = tablesIndexes[tableName][keyName].begin();
			ensureTableIsNotEmpty(row, tablesIndexes[tableName][keyName].end());
			offsetIndex = 0;
		}
		unsigned offset = row->second[offsetIndex];

		Cursor currentRow(row, tablesIndexes[tableName][keyName].end(), offsetIndex, keyName);
		connections[connection.getConnectionId()][tableName] = currentRow;

		return readDataByOffset(tableName, offset);
	}

	json Database::getNextRow(std::string tableName, Connection connection)
	{
		std::shared_lock lock(mutex_);
		unsigned offset = shiftCursorForward(tableName, connection);

		return readDataByOffset(tableName, offset);
	}

	json Database::getPrevRow(std::string tableName, Connection connection)
	{
		std::shared_lock lock(mutex_);
		unsigned offset = shiftCursorBack(tableName, connection);

		return readDataByOffset(tableName, offset);
	}

	void Database::appendRow(std::string tableName, json keyJson, json value, Connection connection)
	{
		std::unique_lock lock(mutex_);
		ensureIsConnected(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		ensureTableExists(tableName, tablesMeta);

		std::ofstream tableFile(tableName + TXT_EXT, std::ios_base::app);
		tableFile.seekp(0, std::ios::end);
		auto pos = tableFile.tellp();

		for (auto key : keyJson.items())
		{
			std::string keyName = key.key();
			loadIndex(tableName, keyName);

			auto curr = tablesIndexes[tableName][keyName].find(key.value());
			auto end = tablesIndexes[tableName][keyName].end();
			if (curr == end)
			{
				tablesIndexes[tableName][keyName].insert({ {key.value(), { (unsigned)pos }} });
			}
			else
			{
				curr->second.push_back((unsigned)pos);
			}

			dumpIndex(tableName, keyName);

			for (auto field : key.value().items())
			{
				value[field.key()] = field.value();
			}
		}

		tableFile << value.dump() << std::endl;
	}

	void Database::removeRow(std::string tableName, Connection connection)
	{
		std::unique_lock lock(mutex_);
		Cursor cursor = getCurrentCursor(tableName, connection);

		unsigned offsetToRemove = cursor.currentRow->second[cursor.offsetIndex];
		unsigned removedLength = 0;
		std::ifstream tableFile(tableName + TXT_EXT);
		tableFile.seekg(offsetToRemove, std::ios::beg);
		std::string toRemoveStr;
		std::getline(tableFile, toRemoveStr);
		unsigned nextEntryOffset = tableFile.tellg();
		tableFile.close();
		removedLength = nextEntryOffset - offsetToRemove;
		json toRemove = json::parse(toRemoveStr);

		try
		{
			shiftCursorForward(tableName, connection);
		}
		catch (DatabaseLib::DatabaseException ex)
		{
			try
			{
				shiftCursorBack(tableName, connection);
			}
			catch (DatabaseLib::DatabaseException ex) {}
		}

		json tablesMeta = readJsonFromFile(META_FILE);
		json associatedKeys = tablesMeta[tableName]["keys"];
		for (auto key : associatedKeys.items())
		{
			std::string keyName = key.key();
			json keyValue;
			for (std::string keyColumn : key.value()) 
			{
				keyValue[keyColumn] = toRemove[keyColumn];
			}
			auto entry = tablesIndexes[tableName][keyName][keyValue];
			if (entry.size() == 1)
			{
				tablesIndexes[tableName][keyName].erase(keyValue);
			}
			else
			{
				entry.erase(std::remove(entry.begin(), entry.end(), offsetToRemove), entry.end());
			}
			for (auto& entry : tablesIndexes[tableName][keyName])
			{
				for (auto& offset : entry.second) {
					if (offset > offsetToRemove)
					{
						offset -= removedLength;
					}
				}
			}
			dumpIndex(tableName, keyName);
		}

		std::ifstream tableFileIn(tableName + TXT_EXT);
		std::string value, rest;
		auto currOffset = tableFileIn.tellg();
		while (std::getline(tableFileIn, value))
		{
			if ((unsigned)currOffset != offsetToRemove)
			{
				rest.append(value);
				rest.append("\n");
			}
			currOffset = (unsigned)tableFileIn.tellg();
		}
		tableFileIn.close();

		std::ofstream tableFileOut(tableName + TXT_EXT);
		tableFileOut << rest;
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
			tablesIndexes[tableName][keyName] = keysMap[keyName];
		}
	}

	void Database::dumpIndex(std::string tableName, std::string keyName)
	{
		json index = json::array();
		for (auto kv : tablesIndexes[tableName][keyName])
		{
			index.push_back({ { keyName, kv.first }, { "offsets", kv.second } });
		}
		std::ofstream indexFile(tableName + "_" + keyName + JSON_EXT);
		indexFile << index.dump();
	}

	void Database::ensureKeyIsFound(std::string tableName, std::string key)
	{
		if (tablesIndexes[tableName].find(key) == tablesIndexes[tableName].end())
		{
			throw DatabaseException("Key not found: " + key , ErrorCode::KEY_NOT_FOUND);
		}
	}

	void Database::ensureDataIsAvailable(Cursor cursor)
	{
		if (cursor.currentRow == cursor.end)
		{
			throw DatabaseException("No more data available", ErrorCode::NO_MORE_DATA_AVAILABLE);
		}
	}

	void Database::ensureIsConnected(Connection connection)
	{
		if (connections.find(connection.getConnectionId()) == connections.end())
		{
			throw DatabaseException("You havent't been connected", ErrorCode::NO_CONNECTION);
		}
	}

	void Database::ensureTableExists(std::string tableName, json tablesMeta)
	{
		if (tablesMeta.is_null() || tablesMeta.empty() || !tablesMeta.contains(tableName))
		{
			throw DatabaseException("Table not found: " + tableName, ErrorCode::TABLE_NOT_FOUND);
		}
	}

	void Database::ensureTableIsNotEmpty(Indexes::iterator row, Indexes::iterator end)
	{
		if (row == end)
		{
			throw DatabaseException("Table is empty", ErrorCode::TABLE_IS_EMPTY);
		}
	}

	Cursor Database::getCurrentCursor(std::string tableName, Connection connection)
	{
		ensureIsConnected(connection);
		json tablesMeta = readJsonFromFile(META_FILE);
		ensureTableExists(tableName, tablesMeta);
		
		Cursor cursor = connections[connection.getConnectionId()][tableName];
		if (cursor.offsetIndex == -1)
		{
			throw DatabaseException("Cursor wasn't opened", ErrorCode::CURSOR_NOT_OPENED);
		}
		loadIndex(tableName, cursor.keyName);

		return cursor;
	}

	unsigned Database::shiftCursorBack(std::string tableName, Connection connection)
	{
		Cursor cursor = getCurrentCursor(tableName, connection);

		if (cursor.offsetIndex == 0)
		{
			cursor.currentRow--;
			ensureDataIsAvailable(cursor);
			auto offsets = cursor.currentRow->second;
			cursor.offsetIndex = offsets.size() - 1;
		}
		else
		{
			cursor.offsetIndex--;
		}
		connections[connection.getConnectionId()][tableName] = cursor;
		return cursor.currentRow->second[cursor.offsetIndex];
	}

	unsigned Database::shiftCursorForward(std::string tableName, Connection connection)
	{
		Cursor cursor = getCurrentCursor(tableName, connection);

		if ((unsigned)cursor.offsetIndex >= cursor.currentRow->second.size() - 1)
		{
			cursor.currentRow++;
			ensureDataIsAvailable(cursor);
			cursor.offsetIndex = 0u;
		}
		else
		{
			cursor.offsetIndex++;
		}
		connections[connection.getConnectionId()][tableName] = cursor;
		return (cursor.currentRow->second)[cursor.offsetIndex];
	}
}