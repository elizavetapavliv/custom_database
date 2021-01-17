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
			keys["emailKey"] = { "email" };

			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);
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
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			
			std::ifstream file("tables_meta.json");
			std::stringstream fileContent;
			fileContent << file.rdbuf();

			json expected;
			expected["clients"]["keys"] = { {"idNameKey", {"id", "name"}}, {"emailKey", {"email"}} };

			Assert::AreEqual(expected.dump(), fileContent.str());
			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(RemoveTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
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
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json keyValue;
			keyValue["emailKey"] = "jh@mail.com";

			bool exceptionIsThrown = false;
			try
			{
				json row = database.getRowByKey("u", keyValue, connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::NOT_FOUND == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(GetRowByKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);
			
			json keyValue;
			keyValue["emailKey"] = "mary@mail.com";
		
			json row = database.getRowByKey("clients", keyValue, connection);

			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Mary";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowByCompositeKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json keyValue;
			keyValue["idNameKey"] = { {"id", 1}, {"name", "John"} };

			json row = database.getRowByKey("clients", keyValue, connection);

			database.removeTable("clients", connection);
			database.disconnect(connection);
			
			std::string expectedMessage = "hello, John";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowByNotExistingKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json keyValue;
			keyValue["addressKey"] = "mary@mail.com";

			bool exceptionIsThrown = false;
			try
			{
				json row = database.getRowByKey("clients", keyValue, connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::NOT_FOUND == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(GetRowByNotExistingKeyValue)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json keyValue;
			keyValue["emailKey"] = "george@mail.com";

			bool exceptionIsThrown = false;
			try
			{
				json row = database.getRowByKey("clients", keyValue, connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::KEY_VALUE_NOT_FOUND == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(GetRowInSortedTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json row = database.getRowInSortedTable("clients", "emailKey", false, connection);

			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Alex";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowInReverseSortedTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json row = database.getRowInSortedTable("clients", "emailKey", true, connection);

			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Mary";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowInSortedTableByCompositeKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json row = database.getRowInSortedTable("clients", "idNameKey", true, connection);

			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, Alex";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetNextRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json keyValue;
			keyValue["emailKey"] = "j23@mail.com";

			database.getRowByKey("clients", keyValue, connection);
			json nextRow = database.getNextRow("clients", connection);

			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, John";

			Assert::AreEqual(expectedMessage, nextRow["message"].get<std::string>());
		}

		TEST_METHOD(NoNextRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			database.getRowInSortedTable("clients", "emailKey", true, connection);

			bool exceptionIsThrown = false;
			try
			{
				json nextRow = database.getNextRow("clients", connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::NO_MORE_DATA_AVAILABLE == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);

			database.removeTable("clients", connection);
			database.disconnect(connection);
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
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json keyValue;
			keyValue["idNameKey"] = { {"id", 1}, {"name", "John"} };

			database.getRowByKey("clients", keyValue, connection);
			json nextRow = database.getNextRow("clients", connection);

			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::string expectedMessage = "bye, John";

			Assert::AreEqual(expectedMessage, nextRow["message"].get<std::string>());
		}

		TEST_METHOD(GetPrevRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json keyValue;
			keyValue["emailKey"] = "jh@mail.com";

			database.getRowByKey("clients", keyValue, connection);
			json prevRow = database.getPrevRow("clients", connection);
			
			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::string expectedMessage = "bye, John";

			Assert::AreEqual(expectedMessage, prevRow["message"].get<std::string>());
		}

		TEST_METHOD(NoPrevRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			database.getRowInSortedTable("clients", "emailKey", false, connection);

			bool exceptionIsThrown = false;
			try
			{
				json nextRow = database.getPrevRow("clients", connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::NO_MORE_DATA_AVAILABLE == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(GetPrevTheSameKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			database.getRowInSortedTable("clients", "idNameKey", true, connection);
			database.getPrevRow("clients", connection);
			database.getPrevRow("clients", connection);
			json prevRow = database.getPrevRow("clients", connection);

			database.removeTable("clients", connection);
			database.disconnect(connection);

			std::string expectedMessage = "hello, John";

			Assert::AreEqual(expectedMessage, prevRow["message"].get<std::string>());
		}

		TEST_METHOD(RemoveRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			database.getRowInSortedTable("clients", "emailKey", false, connection);

			database.removeRow("clients", connection);

			json row = database.getNextRow("clients", connection);
			std::string expectedMessage = "hello, John";
			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(RemoveFirstRowInTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection); 
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			
			database.getRowInSortedTable("clients", "emailKey", false, connection);

			database.removeRow("clients", connection);

			json row = database.getNextRow("clients", connection);
			std::string expectedMessage = "hello, John";
			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(RemoveRowByCompositeKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			json keyValue;
			keyValue["idNameKey"] = { {"id", 1}, {"name", "John"} };
			database.getRowByKey("clients", keyValue, connection);

			database.removeRow("clients", connection);

			json row = database.getRowByKey("clients", keyValue, connection);
			std::string expectedMessage = "bye, John";
			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(AddKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			database.addKey("clients", { {"nameEmailKey", {"name", "email"}} }, connection);

			json row = database.getRowInSortedTable("clients", "nameEmailKey", false, connection);
			std::string expectedMessage = "hello, Alex";
			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(RemoveKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			database.removeKey("clients", "emailKey", connection);

			json keyValue;
			keyValue["emailKey"] = "mary@mail.com";

			bool exceptionIsThrown = false;
			try
			{
				json row = database.getRowByKey("clients", keyValue, connection);
			}
			catch (DatabaseLib::DatabaseException ex)
			{
				Assert::IsTrue(DatabaseLib::ErrorCode::NOT_FOUND == ex.getErrorNumber());
				exceptionIsThrown = true;
			}
			Assert::IsTrue(exceptionIsThrown);

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(InsertRow)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			database.getRowInSortedTable("clients", "emailKey", false, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "bill@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "Bill"}} } }, { {"message", "hello, Bill"} }, connection);
			json row = database.getNextRow("clients", connection); 

			std::string expectedMessage = "hello, Bill";
			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(MultithreadedRead)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			auto readByEmail = [&database]() {
				DatabaseLib::Connection connection = database.connect();
				for (int i = 0; i < 1000; ++i)
				{
					for (std::string email : {"jh@mail.com", "j23@mail.com", "mary@mail.com", "alex@mail.com"})
					{
						json keyValue = { {"emailKey", email} };
						json row = database.getRowByKey("clients", keyValue, connection);
						Assert::AreEqual(email, row["email"].get<std::string>());
					}
				}
			};

			std::thread thread1(readByEmail);
			std::thread thread2(readByEmail);

			thread1.join();
			thread2.join();

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

		TEST_METHOD(MultithreadedReadWriteDelete)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			json keys = { {"emailKey", {"email"}}, {"idNameKey", {"id", "name"}} };
			database.createTable("clients", keys, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "jh@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "hello, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "j23@mail.com"}}},  { "idNameKey", {{"id", 1}, {"name", "John"}} } }, { {"message", "bye, John"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "mary@mail.com"}}}, { "idNameKey", {{"id", 2}, {"name", "Mary"}} } }, { {"message", "hello, Mary"} }, connection);
			database.appendRow("clients", { {"emailKey", {{"email", "alex@mail.com"}}}, { "idNameKey", {{"id", 3}, {"name", "Alex"}} } }, { {"message", "hello, Alex"} }, connection);

			auto readByEmail1 = [&database]() {
				DatabaseLib::Connection connection = database.connect();
				for (int i = 0; i < 1000; ++i)
				{
					database.appendRow("clients", { {"emailKey", {{"email", "bill@mail.com"}}},   { "idNameKey", {{"id", 1}, {"name", "Bill"}} } }, { {"message", "hello, Bill"} }, connection);
					json keyValue = { {"emailKey", "bill@mail.com"} };
					json row = database.getRowByKey("clients", keyValue, connection);
					Assert::AreEqual(std::string("hello, Bill"), row["message"].get<std::string>());
					database.removeRow("clients", connection);
				}
			};

			auto readByEmail2 = [&database]() {
				DatabaseLib::Connection connection = database.connect();
				for (int i = 0; i < 1000; ++i)
				{
					database.appendRow("clients", { {"emailKey", {{"email", "josh@mail.com"}}},   { "idNameKey", {{"id", 4}, {"name", "Josh"}} } }, { {"message", "bye, Josh"} }, connection);
					json keyValue = { {"emailKey", "josh@mail.com"} };
					json row = database.getRowByKey("clients", keyValue, connection);
					Assert::AreEqual(std::string("bye, Josh"), row["message"].get<std::string>());
					database.removeRow("clients", connection);
				}
			};

			std::thread thread1(readByEmail1);
			std::thread thread2(readByEmail2);

			thread1.join();
			thread2.join();

			database.removeTable("clients", connection);
			database.disconnect(connection);
		}

	};
}