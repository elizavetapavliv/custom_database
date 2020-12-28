#pragma once

namespace DatabaseLib
{
	using Indexes = std::map<json, std::vector<unsigned>, JsonComparator>;
	struct Cursor
	{
		Indexes::iterator currentRow;
		Indexes::iterator end;
		int offsetIndex = -1;
		std::string keyName;

		Cursor(Indexes::iterator newCurrentRow, Indexes::iterator end, int offsetIndex, 
			std::string keyName) 
			: currentRow(newCurrentRow), end(end), offsetIndex(offsetIndex), keyName(keyName)
		{}

		Cursor() {}
	};
}