#pragma once

namespace DatabaseLib
{
	struct Cursor
	{
		std::map<json, std::vector<unsigned>, JsonComparator>::iterator currentRow;
		unsigned offsetIndex;

		Cursor(std::map<json, std::vector<unsigned>, JsonComparator>::iterator newCurrentRow, 
			unsigned offsetIndex) : currentRow(newCurrentRow), offsetIndex(offsetIndex)
		{}

		Cursor() {}
	};
}