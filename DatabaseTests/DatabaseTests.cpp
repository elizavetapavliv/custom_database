#include "pch.h"
#include "CppUnitTest.h"
#include "Database.h"
#include "nlohmann/json.hpp"
#include <fstream>

using namespace Microsoft::VisualStudio::CppUnitTestFramework;

namespace DatabaseTests
{
	using json = nlohmann::json;

	TEST_CLASS(DatabaseTests)
	{
	private:
		void createTable(DatabaseLib::Database database)
		{
			json keys;
			keys["idNameKey"] = {"id", "name"};
			keys["emailKey"] = "email";

			database.createTable("clients", keys.dump());
		}
	public:
		TEST_METHOD(ConnectToDatabase)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			Assert::AreEqual(1u, connection.connectionId);
		}

		/*TEST_METHOD(MultiUserConnectToDatabase)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			Assert::AreEqual(2u, connection.connectionId);
			connection = database.connect();
			Assert::AreEqual(3u, connection.connectionId);
		}*/

		TEST_METHOD(CreateTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			createTable(database);
			database.disconnect(connection);

			std::ifstream file("tables_meta.json");
			std::stringstream fileContent;
			fileContent << file.rdbuf();

			json expected;
			expected["clients"]["keys"] = { {"idNameKey", {"id", "name"}}, {"emailKey", "email"} };

			Assert::AreEqual(expected.dump(4), fileContent.str());
		}

		TEST_METHOD(RemoveTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			createTable(database);
			database.removeTable("clients");
			database.disconnect(connection);

			std::ifstream tables_meta("tables_meta.json");
			std::stringstream fileContent;
			fileContent << tables_meta.rdbuf();

			json empty = json::object();

			Assert::AreEqual(empty.dump(), fileContent.str());
		}

		TEST_METHOD(GetRowByKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();
			
			json keyValue;
			keyValue["emailKey"] = "mari@mail.com";

			std::string strRow = database.getRowByKey("users", keyValue.dump());
			database.disconnect(connection);

			json row = json::parse(strRow);
			std::string expectedMessage = "hello, Mari";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowByCompositeKey)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			json keyValue;
			keyValue["idNameKey"] = { {"id", 1}, {"name", "John"} };

			std::string strRow = database.getRowByKey("users", keyValue.dump());
			database.disconnect(connection);

			json row = json::parse(strRow);
			std::string expectedMessage = "hello, Jhon";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

		TEST_METHOD(GetRowInSortedTable)
		{
			DatabaseLib::Database database;
			DatabaseLib::Connection connection = database.connect();

			std::string strRow = database.getRowInSortedTable("users", "emailKey", false);
			database.disconnect(connection);

			json row = json::parse(strRow);
			std::string expectedMessage = "hello, Jhon";

			Assert::AreEqual(expectedMessage, row["message"].get<std::string>());
		}

	};
}