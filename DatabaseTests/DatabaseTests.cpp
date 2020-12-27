#include "pch.h"
#include "CppUnitTest.h"
#include "Database.h"
#include <fstream>

using namespace Microsoft::VisualStudio::CppUnitTestFramework;

namespace DatabaseTests
{
	using json = nlohmann::json;

	TEST_CLASS(DatabaseTests)
	{
	private:
		void createTable(DatabaseLib::Database database, DatabaseLib::Connection connection)
		{
			json keys;
			keys["idNameKey"] = {"id", "name"};
			keys["emailKey"] = "email";

			database.createTable("clients", keys, connection);
		}
	public:
		TEST_METHOD(ConnectToDatabase)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			Assert::AreNotEqual(0u, connection.getConnectionId());
		}

		TEST_METHOD(DisconnectFromDatabase)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			database.disconnect(connection);
			
			bool exceptionIsThrown = false;
			try
			{
				database.createTable("abc", json::object(), connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::NO_CONNECTION == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);
		}

		TEST_METHOD(MultiUserConnectToDatabase)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection1 = database.connect();
			DatabaseLib::Connection connection2 = database.connect();
			Assert::AreNotEqual(connection1.getConnectionId(), connection2.getConnectionId());
		}

		TEST_METHOD(CreateTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			createTable(database, connection);

			std::ifstream file("tables_meta.json");
			std::stringstream fileContent;
			fileContent << file.rdbuf();

			json expected;
			expected["clients"]["keys"] = { {"idNameKey", {"id", "name"}}, {"emailKey", "email"} };

			Assert::AreEqual(expected.dump(), fileContent.str());
			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(RemoveTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			createTable(database, connection);
			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::ifstream tables_meta("tables_meta.json");
			std::stringstream fileContent;
			fileContent << tables_meta.rdbuf();

			json empty = json::object();

			Assert::AreEqual(empty.dump(), fileContent.str());
		}

		TEST_METHOD(NotExistingTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keyValue;
			keyValue["emailKey"] = "jh@mail.com";

			bool exceptionIsThrown = false;
			try
			{
				json row = database.getRowByKey("u", keyValue, connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::TABLE_NOT_FOUND == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);
		}

		TEST_METHOD(GetRowByKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			//createTable(database);
			//init table clients

			json keyValue;
			keyValue["emailKey"] = "mari@mail.com";
		

			//change users to clients
			json row = database.getRowByKey("users", keyValue, connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Mari";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowByCompositeKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keyValue;
			keyValue["idNameKey"] = { {"id", 1}, {"name", "John"} };

			json row = database.getRowByKey("users", keyValue, connection);
			database.disconnect(connection);
			
			std::string expectedMessage = "hello, Jhon";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowByNotExistingKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keyValue;
			keyValue["email"] = "mari@mail.com";

			bool exceptionIsThrown = false;
			try
			{
				json row = database.getRowByKey("users", keyValue, connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::KEY_NOT_FOUND == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);
		}

		TEST_METHOD(GetRowByNotExistingKeyValue)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keyValue;
			keyValue["emailKey"] = "alex@mail.com";

			bool exceptionIsThrown = false;
			try
			{
				json row = database.getRowByKey("users", keyValue, connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::KEY_VALUE_NOT_FOUND == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);
		}

		TEST_METHOD(GetRowInSortedTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json row = database.getRowInSortedTable("users", "emailKey", false, connection);
			database.disconnect(connection);

			std::string expectedMessage = "bye, Jhon";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowInReverseSortedTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json row = database.getRowInSortedTable("users", "emailKey", true, connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Mari";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowInSortedTableByCompositeKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json row = database.getRowInSortedTable("users", "idNameKey", true, connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Mari";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetNextRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keyValue;
			keyValue["emailKey"] = "j23@mail.com";

			database.getRowByKey("users", keyValue, connection);
			json nextRow = database.getNextRow("users", connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Jhon";

			Assert::AreEqual(expectedMessage, nextRow["message"].get<std::string>());
		}

		TEST_METHOD(NoNextRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			database.getRowInSortedTable("users", "emailKey", true, connection);

			bool exceptionIsThrown = false;
			try
			{
				json nextRow = database.getNextRow("users", connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::NO_MORE_DATA_AVAILABLE == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);
		}

		TEST_METHOD(CursorNotOpened)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			bool exceptionIsThrown = false;
			try
			{
				database.createTable("clients", json::object(), connection);
				json nextRow = database.getNextRow("clients", connection);
				database.removeTable("clients", connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::CURSOR_NOT_OPENED == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);
		}

		TEST_METHOD(GetNextRowTheSameKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keyValue;
			keyValue["idNameKey"] = { {"id", 1}, {"name", "John"} };

			database.getRowByKey("users", keyValue, connection);
			json nextRow = database.getNextRow("users", connection);
			database.disconnect(connection);

			std::string expectedMessage = "bye, Jhon";

			Assert::AreEqual(expectedMessage, nextRow["message"].get<std::string>());
		}

		TEST_METHOD(GetPrevRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keyValue;
			keyValue["emailKey"] = "jh@mail.com";

			database.getRowByKey("users", keyValue, connection);
			json prevRow = database.getPrevRow("users", connection);
			database.disconnect(connection);

			std::string expectedMessage = "bye, Jhon";

			Assert::AreEqual(expectedMessage, prevRow["message"].get<std::string>());
		}

		TEST_METHOD(NoPrevRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			database.getRowInSortedTable("users", "emailKey", false, connection);

			bool exceptionIsThrown = false;
			try
			{
				json nextRow = database.getPrevRow("users", connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::NO_MORE_DATA_AVAILABLE == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);
		}

		TEST_METHOD(GetPrevTheSameKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			database.getRowInSortedTable("users", "idNameKey", true, connection);
			database.getPrevRow("users", connection);
			json prevRow = database.getPrevRow("users", connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Jhon";

			Assert::AreEqual(expectedMessage, prevRow["message"].get<std::string>());
		}

	};
}